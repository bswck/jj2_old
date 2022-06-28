import abc
import asyncio
import enum
import functools
import struct
from typing import Literal

from construct import (
    Enum, PaddedString, Byte, Bytes, GreedyBytes, Struct, PascalString, this, Switch, Int,
    Optional, Pass, PrefixedArray, Short, Default, CString, GreedyRange, Int32ul, Int16ul,
    GreedyString, Int8sb, Bitwise, BitsSwapped, Array, OneOf, If as ConstructIf, BitStruct,
    Padding
)
from construct import possiblestringencodings

from jj2.lib import AbstractPayload, Protocol, If, Priority, Client
from jj2.lib import handles
from jj2.lib import ALL_PAYLOADS
from jj2.lib.context import Context, Item

MAIN_ENCODING = 'cp1250'
possiblestringencodings[MAIN_ENCODING] = 1


class MajorVersionString(str, enum.Enum):
    v1_23 = '21  '
    v1_24 = '24  '

    def as_tuple(self):
        if self is self.v1_24:
            return 1, 24
        return 1, 23


class Character(enum.IntEnum):
    JAZZ = 0
    SPAZ = 1
    BIRD = 2
    LORI = FROG = 3


class Team(enum.IntEnum):
    BLUE = 0
    RED = 1


class GameMode(enum.IntEnum):
    SINGLEPLAYER = 0
    COOP = 1
    BATTLE = 2
    RACE = 3
    TREASURE_HUNT = 4
    CTF = 5


class CustomGameMode(enum.IntEnum):
    OFF = 0
    ROAST_TAG = 1
    LRS = 2
    XLRS = 3
    PESTILENCE = 4
    TEAM_BATTLE = 11
    JAILBREAK = 12
    DEATH_CTF = 13
    FLAG_RUN = 14
    TLRS = 15
    DOMINATION = 16


class ChatType(enum.IntEnum):
    NORMAL = 0
    TEAM_CHAT = 1
    WHISPER = 2
    ME = 3


class GameEventType(enum.IntEnum):
    PLAYER_GOT_ROASTED = 10
    BULLET_SHOT = 16


class SpectateTarget(enum.IntEnum):
    BOTTOM_FEEDER = -6
    RING_HOLDER = -5
    BLUE_FLAG = -4
    RED_FLAG = -3
    STOP_SPECTATING_1 = -2
    START_SPECTATING_1 = -1
    START_SPECTATING_2 = 0
    STOP_SPECTATING_2 = 33
    START_SPECTATING_3 = 34


class PlusTimestamp(enum.Enum):
    v4_1 = 0x20, 0x01, 0x01, 0x00, 0x04, 0x00  # 2013-02-05  rerelease from 2013-02-04
    v5_7 = 0x20, 0x03, 0x00, 0x00, 0x06, 0x00  # 2020-08-16
    v5_9 = 0x20, 0x03, 0x09, 0x00, 0x05, 0x00  # 2021-05-11

    latest = v5_9


MajorVersionStringEnum = Enum(PaddedString(4, 'ascii'), MajorVersionString)
CharacterEnum = Enum(Byte, Character)
TeamEnum = Enum(Byte, Team)
GameModeEnum = Enum(Byte, GameMode)
CustomGameModeEnum = Enum(Byte, CustomGameMode)
GameEventTypeEnum = Enum(Byte, GameEventType)
ChatTypeEnum = Enum(Byte, ChatType)
SpectateTargetEnum = Enum(Int8sb, SpectateTarget)
PlusTimestampEnum = Enum(Bytes(6), PlusTimestamp)


DISCONNECT_MESSAGES = {
    'Unknown error': 1,
    'Version different': 2,
    'Server is full': 3,
    'Error during handshaking': 4,
    'Feature not supported in shareware': 5,
    'Error downloading level': 6,
    'Connection lost': 7,
    'Winsock error': 8,
    'Connection timed out': 9,
    'Server stopped': 10,
    'Kicked off': 11,
    'Banned': 12,
    'Denied': 13,  # Starting from 13 are JJ2+
    'Version of JJ2+ is different': 14,
    'Server kicked you for idling': 15,
    'No downloads allowed': 16,
    'Unauthorized file request': 17,
    'No splitscreeners allowed': 18,
    'Kicked for spamming': 19
}


def player_array(*, with_client_id):
    kwds = {}
    if with_client_id:
        kwds.update(client_id=Byte)
    kwds.update(
        player_id=Byte,
        team=TeamEnum,
        char=Byte,
        fur_color=Byte[4],
        sprite_mode=Default(Byte, 17),
        sprite_mode_param=Default(Byte, this.player_id),
        light_type=Default(Byte, 10),
        light_size=Default(Byte, 13),
        antigrav_and_nofire=Default(Byte, 0),
        unused=Default(Byte, 0),
        rabbit_name=CString(MAIN_ENCODING)
    )
    return Struct(**kwds)


class GameProtocol(Protocol, asyncio.Protocol, asyncio.DatagramProtocol):
    def __init__(
            self,
            parent=None,
            *,
            engine,
            future,
            bot=True,
            capture_packets=True,
            is_client=True,
            chat=True,
            notice_players=True,
            download_files=True,
            passwords=True,
            spectating=True,
            update_latencies=True,
            **config
    ):
        super().__init__(
            parent,
            bot=bot,
            capture_packets=capture_packets,
            is_client=is_client,
            chat=chat,
            notice_players=notice_players,
            download_files=download_files,
            passwords=passwords,
            spectating=spectating,
            update_latencies=update_latencies,
            **config
        )
        self.engine = engine
        self.context = GameContext(self)
        self._future = future
        self._deficit = 0
        self._buffer = bytearray()
        self._tcp_transport = None
        self._udp_transport = None

    @property
    def future(self):
        return self._future

    def connection_made(self, transport):
        if hasattr(transport, 'sendto'):
            self._udp_transport = transport
        else:
            self._tcp_transport = transport

    def send(self, data: bytes):
        length = len(data) + 1
        arr = bytearray()
        try:
            lsb = Byte.build(length)
        except struct.error:
            arr.append(0)
            lsb = Int16ul.build(length)
        arr.append(lsb)
        arr.extend(data)
        return self._tcp_transport.write(arr)

    def sendto(self, data: bytes):
        return self._udp_transport.sendto(data)

    def submit_all(self, *payloads):
        for payload in payloads:
            self.submit(payload)

    def submit(self, payload):
        ip = payload.ip.lower()
        if ip == 'tcp':
            self.send(payload.serialize(self.context))
        if ip == 'udp':
            self.sendto(payload.serialize(self.context, checksum=True))
        return self

    def data_received(self, data: bytes):
        length = eof = len(data)
        deficit = self._deficit

        if deficit == 0:
            bof = 1
            if data[0] == 0:
                bof += 2
                elength = Int16ul.parse(data[1:3]) + 3
            else:
                elength = data[0]
            diff = elength - length
            if diff < 0:
                eof += diff
                diff = 0
            deficit = diff
        elif deficit > 0:
            bof = 0
            if length < deficit:
                deficit -= length
            else:
                eof -= deficit
                deficit = 0
        else:
            raise ValueError(f'{deficit=} < 0')

        self._buffer.extend(data[bof:eof])
        self._deficit = deficit

        if deficit == 0:
            payload = GamePayload.load(buffer=bytes(self._buffer))
            self.handle(payload)
            self._buffer.clear()
            if eof < length:
                tail = data[eof:]
                self.data_received(tail)

    def datagram_received(self, data: bytes, addr: tuple):
        payload = GamePayload.load(buffer=bytes(data), checksum=data[:2])
        if payload:
            self.handle(payload)

    def eof_received(self):
        self.engine.dispatch(self, 'eof')
        return False

    def error_received(self, exc):
        super().on_error(msg=f'send/receive operation of UDP ({exc})')

    @classmethod
    def register(
            cls,
            registered=None,
            condition=None, *,
            ip: Literal['tcp', 'udp'] = None
    ):
        if registered is None:
            return functools.partial(cls.register, registered, condition, ip=ip)
        if ip is None:
            raise ValueError('ip (internet protocol) must be either TCP or UDP')
        registered = super().register(registered, condition)
        registered.ip = ip
        return registered


def _collect_values_from_struct(subcons, context):
    result = {}
    for subcon in subcons:
        if context is None:
            return result
        value = context.get(subcon.name)
        if isinstance(subcon, Struct):
            value = _collect_values_from_struct(subcon.subcons, value)
        result[subcon.name] = value
    return result


class BinaryPayload(AbstractPayload, abc.ABC, has_feed=False):
    struct = Struct(buffer=GreedyBytes)
    feeds = 'buffer'
    has_default_implementation = True

    def _serialize(self, context):
        return self.struct.build(self.data(deserialization=False))

    def _deserialize(self, buffer, context):
        data = self.struct.parse(buffer)
        del data['_io']
        return data

    @classmethod
    def from_context(cls, context):
        return cls(**_collect_values_from_struct(cls.struct.subcons, context))

    def __init_subclass__(cls, compile_structs=True, feeds=None):
        if compile_structs and cls.struct is not None:
            cls.struct = cls.struct.compile()
        if feeds:
            cls.feeds = feeds
        super().__init_subclass__()


class GamePayload(BinaryPayload):
    struct = Struct(
        packet_id=Byte,
        buffer=GreedyBytes
    )
    has_default_implementation = False

    def _pick(self, context):
        return self.impls.get(self._data['packet_id'])

    def _impl_data(self, deserialization=False):
        return self._data['buffer']

    def serialize(self, context=None, checksum=False):
        if self.serialize_cache is None:
            buffer = super().serialize(context)
            if checksum:
                buffer = self.checksum(buffer) + buffer
            self.feed(dict(buffer=buffer))
            self.serialize_cache = self._serialize(context)
        return self.serialize_cache

    @staticmethod
    def checksum(buffer):
        arr = bytes((79, 79)) + buffer
        lsb = msb = 1
        for i in range(2, len(arr)):
            lsb += arr[i]
            msb += lsb
        return Byte.build(lsb % 251) + Byte.build(msb % 251)

    @classmethod
    def load(cls, buffer, checksum=None, context=None):
        self = super().load(buffer=buffer, context=context)
        if checksum is not None:
            if self.checksum() != checksum:
                self = None
        return self


packet_id = GamePayload.register


@packet_id(0x03)
class Ping(BinaryPayload):
    event = 'ping'
    struct = Struct(
        number_in_list=Byte,
        unknown_data=Byte[4],
        client_version=MajorVersionStringEnum
    )


@packet_id(0x04)
class Pong(BinaryPayload):
    event = 'pong'
    struct = Struct(
        number_in_list_from_ping=Byte,
        unknown_data=Byte[4],
        game_mode_etc=Byte
    )


@packet_id(0x05)
class Query(BinaryPayload):
    event = 'query'
    struct = Struct(
        number_in_list=Byte
    )


@packet_id(0x06)
class QueryReply(BinaryPayload):
    event = 'query_reply'
    struct = Struct(
        number_in_list=Byte,
        timer_sync=Byte,
        laps_on_timer_sync=Byte,
        unknown_data_1=Byte[2],
        client_version=MajorVersionStringEnum,
        player_count=Byte,
        unknown_data_2=Byte,
        game_mode=GameMode,
        player_limit=Byte,
        server_name=PascalString(Byte, MAIN_ENCODING),
        unknown_data_3=Byte
    )


@packet_id(0x07)
class GameEvent(BinaryPayload):
    event = 'game_event'
    struct = Struct(
        udp_count=Byte,
        event_id=GameEventTypeEnum,
        event_data=GreedyBytes
    )


@packet_id(0x09)
class Heartbeat(BinaryPayload):
    event = 'heartbeat'
    struct = Struct(
        udp_count=Byte,
        send_back=GreedyBytes,
    )


@packet_id(0x0A)
class Password(BinaryPayload):
    event = 'password'
    struct = Struct(password=PascalString(Byte, MAIN_ENCODING))


@packet_id(0x0B)
class PasswordCheck(BinaryPayload):
    event = 'password_check'
    struct = Struct(password_ok=Byte)


@packet_id(0x0D)
class ClientDisconnect(BinaryPayload):
    event = 'disconnect'
    struct = Struct(
        disconnect_message=Enum(Byte, **DISCONNECT_MESSAGES),
        client_id=Int8sb,
        client_version=MajorVersionStringEnum,
        include_reason=Optional(Byte),
        reason=ConstructIf(
            this.include_reason,
            PascalString(Byte, MAIN_ENCODING)
        )
    )


@packet_id(0x0F)
class JoinRequest(BinaryPayload):
    event = 'join_request'
    struct = Struct(
        udp_bind=Default(Short, 10052),
        client_version=Default(MajorVersionStringEnum, MajorVersionString.v1_24),
        number_of_players_from_client=Default(Byte, 1)
    )


@packet_id(0x10)
class ServerDetails(BinaryPayload):
    event = 'server_details'
    struct = Struct(
        client_id=Byte,
        player_id=Byte,
        level_file_name=PascalString(Byte, MAIN_ENCODING),
        level_crc=Int,
        tileset_crc=Int,
        game_mode=GameMode,
        max_score=Byte,
        plus_specific=Optional(
            Struct(
                level_challenge=Byte[4],
                keep_alive_data=Byte[4],
                plus_version=Short[2],
                music_crc=Switch(Byte, dict(enumerate([Pass, PrefixedArray(Byte, Byte)]))),
                scripts=Switch(
                    Byte, dict(map(tuple, enumerate([
                        Pass,
                        Struct(script_crc=Byte[4]),
                        Struct(
                            number_of_script_files=Byte,
                            number_of_required_files=Byte,
                            number_of_optional_files=Byte
                        )
                    ])))
                )
            )
        )
    )


@packet_id(0x11)
class ClientDetails(BinaryPayload):
    event = 'client_details'
    struct = Struct(
        client_id=Byte,
        players=PrefixedArray(Byte, player_array(with_client_id=False))
    )


@packet_id(0x12)
class UpdatePlayers(BinaryPayload):
    event = 'players'
    struct = Struct(
        junk=Byte,
        players=GreedyRange(player_array(with_client_id=True))
    )


@packet_id(0x13)
class GameInit(BinaryPayload):
    event = 'game_init'
    struct = Struct()


@packet_id(0x14)
class DownloadingFile(BinaryPayload):
    event = 'downloading_file'

    def _pick(self, context):
        return self.impls.get(context.get('is_downloading', False))


@DownloadingFile.register(True)
class _DownloadingFileInit(BinaryPayload):
    event = 'downloading_file'
    struct = Struct(
        packet_count=Int32ul,
        unknown_data=Byte[4],
        file_name=PascalString(Byte, MAIN_ENCODING)
    )


@DownloadingFile.register(False)
class _DownloadingFileChunk(BinaryPayload):
    event = 'downloading_file'
    struct = Struct(
        packet_count=Int32ul,
        file_content=GreedyBytes
    )


@packet_id(0x15)
class DownloadRequest(BinaryPayload):
    event = 'download_request'
    struct = Struct(file_name=PascalString(Byte, MAIN_ENCODING))


@packet_id(0x16)
class LevelLoad(BinaryPayload):
    event = 'level_load'
    struct = Struct(
        level_crc=Int,
        tileset_crc=Int,
        level_file_name=PascalString(Byte, MAIN_ENCODING),
        level_challenge=Byte[4],
        is_different=Byte,
        music=Byte,
        music_crc=Byte[4],
        script_data=Optional(Byte[5])
    )


@packet_id(0x17)
class EndOfLevel(BinaryPayload):
    event = 'end_of_level'
    struct = Struct(unknown_data=GreedyBytes)


@packet_id(0x18)
class UpdateEvents(BinaryPayload):
    event = 'update_events'
    struct = Struct(
        checksum=Optional(Short),
        counter=Optional(Short),
        unknown_data=Optional(GreedyString(MAIN_ENCODING))
    )


@packet_id(0x19)
class ServerStopped(BinaryPayload):  # rip
    event = 'stopped'
    struct = Struct()


@packet_id(0x1A)
class UpdateRequest(BinaryPayload):
    event = 'update_request'
    struct = Struct(
        level_challenge=Byte[4],
    )


@packet_id(0x1B)
class ChatMessage(BinaryPayload):
    event = 'chat'
    struct = Struct(
        client_id=Byte,
        chat_type=ChatTypeEnum,
        text=GreedyString(MAIN_ENCODING),
    )


@packet_id(0x3F)
class PlusAcknowledgement(BinaryPayload):
    event = 'plus'
    
    def _pick(self, context):
        return self.impls.get(context.get('client', False))

    def _impl_data(self, deserialization=False):
        return self._data['buffer']


@PlusAcknowledgement.register(True)  # client-side
class PlusRequest(BinaryPayload):
    event = 'plus_request'
    struct = Struct(timestamp=Default(PlusTimestampEnum, PlusTimestamp.latest))


@PlusAcknowledgement.register(False)  # server-side
class PlusDetails(BinaryPayload):
    event = 'plus_details'
    struct = Struct(
        unknown=Byte,
        health_info=Byte,
        plus_data=BitStruct(
            pad=Padding(4),
            no_blink=Byte,
            no_movement=Byte,
            friendly_fire=Byte,
            plus_only=Byte
        )
    )


@packet_id(0x40)
class ConsoleMessage(BinaryPayload):
    event = 'console'
    struct = Struct(
        message_type=Byte,
        text=GreedyString(MAIN_ENCODING)
    )


@packet_id(0x41)
class Spectate(BinaryPayload):
    event = 'spectate_pkt'
    struct = Struct(
        packet_type=Byte,
        buffer=GreedyBytes
    )

    def _pick(self, context):
        return self.impls[self._data['packet_type']]

    def _impl_data(self, deserialization=False):
        return self._data['buffer']


@Spectate.register(0)
class _EachSpectator(BinaryPayload):
    event = 'all_spectators'
    struct = Struct(spectators=Array(4, BitsSwapped(Bitwise(Bytes(8)))))


@Spectate.register(1)
class _Spectators(BinaryPayload):
    event = 'spectators'
    struct = Struct(
        spectators=GreedyRange(
            Struct(
                is_out=Byte,
                client_id=Byte,
                spectate_target=SpectateTargetEnum
            )
        )
    )


@packet_id(0x42)
class SpectateRequest(BinaryPayload):
    event = 'spectate'
    struct = Struct(
        spectating=OneOf(Byte, (20, 21))
    )

    def feed(self, changes):
        if 'spectating' in changes:
            changes['spectating'] = 20 + (changes['spectating'] % 2)
        super().feed(changes)


@packet_id(0x45)
class GameState(BinaryPayload):
    event = 'game_state'
    struct = Struct(
        state=BitStruct(
            pad=Padding(5),
            in_overtime=Bytes(2),
            game_started=Byte
        ),
        time_left=Int
    )


@packet_id(0x49)
class Latency(BinaryPayload):
    event = 'latencies'
    struct = Struct(
        latencies=GreedyRange(
            Struct(
                player_id=Byte,
                latency=Short
            )
        )
    )

    def data(self, deserialization=False, to_impl=False):
        data = super().data(deserialization, to_impl)
        if deserialization:
            for latency_details in data['latencies']:
                latency_details['latency'] >>= 8
        return data


@packet_id(0x51)
class UpdateReady(BinaryPayload):
    event = 'ready'
    struct = Struct()


@packet_id(0x5A)
class ScriptList(BinaryPayload):
    event = 'scripts'
    struct = Struct(
        level_challenge=Byte[4],
        script_data=Byte[5],
        scripts=GreedyRange(
            Struct(
                script_data=Byte[5],
                name=PascalString(Byte, MAIN_ENCODING),
            )
        )
    )


GameProtocol.register(ChatMessage, If.configured(chat=True), ip='tcp')
GameProtocol.register(ClientDetails, If.configured(notice_players=True), ip='tcp')
GameProtocol.register(ClientDisconnect, If.configured(notice_players=True), ip='tcp')
GameProtocol.register(ConsoleMessage, If.configured(chat=True), ip='tcp')
GameProtocol.register(DownloadingFile, If.configured(download_files=True), ip='tcp')
GameProtocol.register(DownloadRequest, If.configured(download_files=True), ip='tcp')
GameProtocol.register(EndOfLevel, ip='tcp')
GameProtocol.register(GameEvent, ip='udp')
GameProtocol.register(GameInit, ip='tcp')
GameProtocol.register(GameState, ip='udp')
GameProtocol.register(Heartbeat, ip='udp')
GameProtocol.register(JoinRequest, ip='tcp')
GameProtocol.register(Latency, If.configured(update_latencies=True), ip='tcp')
GameProtocol.register(LevelLoad, ip='tcp')
GameProtocol.register(Password, If.configured(passwords=True), ip='udp')
GameProtocol.register(PasswordCheck, If.configured(passwords=True), ip='udp')
GameProtocol.register(Ping, ip='udp')
GameProtocol.register(PlusAcknowledgement, If.configured(latest_plus=True), ip='tcp')
GameProtocol.register(Pong, ip='udp')
GameProtocol.register(Query, ip='udp')
GameProtocol.register(QueryReply, ip='udp')
GameProtocol.register(ScriptList, ip='tcp')
GameProtocol.register(ServerDetails, ip='tcp')
GameProtocol.register(ServerStopped, ip='tcp')
GameProtocol.register(Spectate, If.configured(spectating=True), ip='tcp')
GameProtocol.register(SpectateRequest, If.configured(spectating=True), ip='tcp')
GameProtocol.register(UpdateEvents, ip='tcp')
GameProtocol.register(UpdatePlayers, If.configured(notice_players=True), ip='tcp')
GameProtocol.register(UpdateReady, ip='tcp')
GameProtocol.register(UpdateRequest, ip='tcp')


@GameProtocol.handles(ALL_PAYLOADS)
def dispatch(protocol, payload):
    protocol.engine.dispatch(protocol, payload)


@GameProtocol.handles(ALL_PAYLOADS, If.configured(bot=True))
class BotProtocol(Protocol, extends=GameProtocol):
    """Packet coordination in the background using default bot behavior."""

    def __init__(
            self,
            parent=None, *,
            join_servers=True,
            autospectate=True,
            **config
    ):
        if parent is None:
            raise ValueError(
                'the bot protocol relies on running instance of the gameplay protocol'
            )
        super().__init__(
            parent,
            join_servers=join_servers,
            autospectate=autospectate,
            **config
        )
        self.context = self.parent.context

    @handles(ServerDetails, response=ClientDetails, priority=Priority.URGENT)
    def on_server_details(self, payload, response):
        self.context.update(payload.data())
        self.parent.submit(response.from_context(self.context))


class GameContext(Context):
    client_id = Item()
    players = Item()


class GameClient(Client):
    def __init__(self, **config):
        super().__init__(**config)
        self.config['is_client'] = True

    async def run(self, timeout=None):
        futs = (proto.future for protos in self.protocols.values() for proto in protos)
        await asyncio.wait(futs, timeout=timeout)

    async def connect(self, host, port=10052):
        protocol = GameProtocol(engine=self, future=self.loop.create_future(), **self.config)
        await self.loop.create_connection(lambda: protocol, host=host, port=port)
        await self.loop.create_datagram_endpoint(lambda: protocol, remote_addr=(host, port))
        return protocol

    async def join(self, protocol):
        protocol.submit_all(
            JoinRequest(), 
            PlusRequest(), 
        )
