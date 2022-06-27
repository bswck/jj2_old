import abc
import asyncio
import enum
import functools

from construct import (
    Enum, PaddedString, Byte, Bytes, GreedyBytes, Struct, PascalString, this, Switch, Int,
    Optional, Pass, PrefixedArray, Short, Default, CString, GreedyRange, Int32ul, Int16ul,
    GreedyString, Int8sb, Bitwise, BitsSwapped, Array, OneOf, If as ConstructIf, BitStruct,
    Padding, possiblestringencodings
)

from jj2.lib import AbstractPayload, Protocol, If, ALL_PAYLOADS, relation

MAIN_ENCODING = 'cp1250'
possiblestringencodings[MAIN_ENCODING] = 1


@functools.partial(Enum, PaddedString(4, 'ascii'))
class MajorVersionString(str, enum.Enum):
    v1_23 = '21  '
    v1_24 = '24  '

    def as_tuple(self):
        if self is self.v1_24:
            return 1, 24
        return 1, 23


@functools.partial(Enum, Byte)
class Character(enum.IntEnum):
    JAZZ = 0
    SPAZ = 1
    BIRD = 2
    LORI = FROG = 3


@functools.partial(Enum, Byte)
class Team(enum.IntEnum):
    BLUE = 0
    RED = 1


@functools.partial(Enum, Byte)
class GameMode(enum.IntEnum):
    SINGLEPLAYER = 0
    COOP = 1
    BATTLE = 2
    RACE = 3
    TREASURE_HUNT = 4
    CTF = 5


@functools.partial(Enum, Byte)
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


@functools.partial(Enum, Byte)
class GameEventType(enum.IntEnum):
    PLAYER_GOT_ROASTED = 10
    BULLET_SHOT = 16


@functools.partial(Enum, Int8sb)
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


@functools.partial(Enum, Bytes(6))
class PlusTimestamp(enum.Enum):
    v4_1 = 0x20, 0x01, 0x01, 0x00, 0x04, 0x00  # 2013-02-05  rerelease from 2013-02-04
    v5_7 = 0x20, 0x03, 0x00, 0x00, 0x06, 0x00  # 2020-08-16
    v5_9 = 0x20, 0x03, 0x09, 0x00, 0x05, 0x00  # 2021-05-11

    latest = v5_9


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
        team=Team,
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


class GameplayProtocol(Protocol, asyncio.Protocol):
    def __init__(
            self,
            parent=None, *,
            capture_packets=True,
            chat=True,
            notice_players=True,
            download_files=True,
            update_latencies=True,
            spectating=True,
            bot=True,
            **config
    ):
        super().__init__(
            parent,
            capture_packets=capture_packets,
            chat=chat,
            notice_players=notice_players,
            download_files=download_files,
            update_latencies=update_latencies,
            spectating=spectating,
            bot=bot,
            **config
        )
        self._deficit = 0
        self._buffer = bytearray()
        self._tcp_transport = None
        self._udp_transport = None

    def connection_made(self, transport):
        if hasattr(transport, 'sendto'):
            self._udp_transport = transport
        else:
            self._tcp_transport = transport

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
            payload = GameplayPayload.load(buffer=bytes(self._buffer))
            self.handle(payload)
            self._buffer.clear()
            if eof < length:
                tail = data[eof:]
                self.data_received(tail)

    def datagram_received(self, data: bytes):
        payload = GameplayPayload.load(buffer=bytes(data), checksum=data[:2])
        if payload:
            self.handle(payload)


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

    def __init_subclass__(cls, compile_struct=not __debug__, feeds=None):
        if compile_struct and cls.struct is not None:
            cls.struct = cls.struct.compile()
        if feeds:
            cls.feeds = feeds
        super().__init_subclass__()


class GameplayPayload(BinaryPayload):
    struct = Struct(
        packet_id=Byte,
        buffer=GreedyBytes
    )
    has_default_implementation = False

    def _pick(self, context):
        return self.impls.get(self._data['packet_id'])

    def _impl_data(self, deserialization=False):
        return self._data['buffer']

    def serialize(self, context, checksum=False):
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


packet_id = GameplayPayload.register


@packet_id(0x03)
class Ping(BinaryPayload):
    struct = Struct(
        number_in_list=Byte,
        unknown_data=Byte[4],
        client_version=MajorVersionString
    )


@packet_id(0x04)
class Pong(BinaryPayload):
    struct = Struct(
        number_in_list_from_ping=Byte,
        unknown_data=Byte[4],
        game_mode_etc=Byte
    )


@packet_id(0x05)
class Query(BinaryPayload):
    struct = Struct(
        number_in_list=Byte
    )


@packet_id(0x06)
class QueryReply(BinaryPayload):
    struct = Struct(
        number_in_list=Byte,
        timer_sync=Byte,
        laps_on_timer_sync=Byte,
        unknown_data_1=Byte[2],
        client_version=MajorVersionString,
        player_count=Byte,
        unknown_data_2=Byte,
        game_mode=GameMode,
        player_limit=Byte,
        server_name=PascalString(Byte, MAIN_ENCODING),
        unknown_data_3=Byte
    )


@packet_id(0x07)
class GameEvent(BinaryPayload):
    struct = Struct(
        udp_count=Byte,
        event_id=GameEventType,
        event_data=GreedyBytes
    )


@packet_id(0x09)
class Heartbeat(BinaryPayload):
    struct = Struct(
        udp_count=Byte,
        send_back=GreedyBytes,
    )


@packet_id(0x0D)
class ClientDisconnect(BinaryPayload):
    struct = Struct(
        disconnect_message=Enum(Byte, **DISCONNECT_MESSAGES),
        client_id=Int8sb,
        client_version=MajorVersionString,
        include_reason=Optional(Byte),
        reason=ConstructIf(
            this.include_reason,
            PascalString(Byte, MAIN_ENCODING)
        )
    )


@packet_id(0x0F)
class JoinRequest(BinaryPayload):
    struct = Struct(
        udp_bind=Short,
        client_version=MajorVersionString,
        number_of_players_from_client=Byte
    )


@packet_id(0x10)
class ServerDetails(BinaryPayload):
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
    struct = Struct(
        client_id=Byte,
        players=PrefixedArray(Byte, player_array(with_client_id=False))
    )


@packet_id(0x12)
class UpdatePlayers(BinaryPayload):
    struct = Struct(
        junk=Byte,
        players=GreedyRange(player_array(with_client_id=True))
    )


@packet_id(0x13)
class GameInit(BinaryPayload):
    struct = Struct()


@packet_id(0x14)
class DownloadingFile(BinaryPayload):
    def _pick(self, context):
        return self.impls.get(context.get('is_downloading', False))


@DownloadingFile.register(True)
class _DownloadingFileInit(BinaryPayload):
    struct = Struct(
        packet_count=Int32ul,
        unknown_data=Byte[4],
        file_name=PascalString(Byte, MAIN_ENCODING)
    )


@DownloadingFile.register(False)
class _DownloadingFileChunk(BinaryPayload):
    struct = Struct(
        packet_count=Int32ul,
        file_content=GreedyBytes
    )


@packet_id(0x15)
class DownloadRequest(BinaryPayload):
    struct = Struct(file_name=PascalString(Byte, MAIN_ENCODING))


@packet_id(0x16)
class LevelCycled(BinaryPayload):
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
    struct = Struct(unknown_data=GreedyBytes)


@packet_id(0x18)
class UpdateEvents(BinaryPayload):
    struct = Struct(
        checksum=Optional(Short),
        counter=Optional(Short),
        unknown_data=Optional(GreedyString(MAIN_ENCODING))
    )


@packet_id(0x19)
class ServerStopped(BinaryPayload):  # rip
    struct = Struct()


@packet_id(0x1A)
class UpdateRequest(BinaryPayload):
    struct = Struct(
        level_challenge=Byte[4],
    )


@packet_id(0x1B)
class ChatMessage(BinaryPayload):
    struct = Struct(
        client_id=Byte,
        is_team_chat=Byte,
        text=GreedyString(MAIN_ENCODING),
    )


@packet_id(0x3F)
class PlusAcknowledgement(BinaryPayload):
    def _pick(self, context):
        return self.impls.get(context.get('client', False))

    def _impl_data(self, deserialization=False):
        return self._data['buffer']


@PlusAcknowledgement.register(True)  # client-side
class PlusRequest(BinaryPayload):
    struct = Struct(timestamp=PlusTimestamp)


@PlusAcknowledgement.register(False)  # server-side
class PlusDetails(BinaryPayload):
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
    struct = Struct(
        message_type=Byte,
        text=GreedyString(MAIN_ENCODING)
    )


@packet_id(0x41)
class Spectate(BinaryPayload):
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
    struct = Struct(spectators=Array(4, BitsSwapped(Bitwise(Bytes(8)))))


@Spectate.register(1)
class _Spectators(BinaryPayload):
    struct = Struct(
        spectators=GreedyRange(
            Struct(
                is_out=Byte,
                client_id=Byte,
                spectate_target=SpectateTarget
            )
        )
    )


@packet_id(0x42)
class SpectateRequest(BinaryPayload):
    struct = Struct(
        spectating=OneOf(Byte, (20, 21))
    )

    def feed(self, changes):
        if 'spectating' in changes:
            changes['spectating'] = 20 + (changes['spectating'] % 2)
        super().feed(changes)


@packet_id(0x45)
class GameState(BinaryPayload):
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
    struct = Struct()


@packet_id(0x5A)
class ScriptList(BinaryPayload):
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


GameplayProtocol.register(ChatMessage, If.configured(chat=True))
GameplayProtocol.register(ClientDetails, If.configured(notice_players=True))
GameplayProtocol.register(ClientDisconnect, If.configured(notice_players=True))
GameplayProtocol.register(ConsoleMessage, If.configured(chat=True))
GameplayProtocol.register(DownloadingFile, If.configured(download_files=True))
GameplayProtocol.register(DownloadRequest, If.configured(download_files=True))
GameplayProtocol.register(EndOfLevel)
GameplayProtocol.register(GameEvent)
GameplayProtocol.register(GameInit)
GameplayProtocol.register(GameState)
GameplayProtocol.register(Heartbeat)
GameplayProtocol.register(JoinRequest)
GameplayProtocol.register(Latency, If.configured(update_latencies=True))
GameplayProtocol.register(LevelCycled)
GameplayProtocol.register(Ping)
GameplayProtocol.register(PlusAcknowledgement, If.configured(latest_plus=True))
GameplayProtocol.register(Pong)
GameplayProtocol.register(Query)
GameplayProtocol.register(QueryReply)
GameplayProtocol.register(ScriptList)
GameplayProtocol.register(ServerDetails)
GameplayProtocol.register(ServerStopped)
GameplayProtocol.register(Spectate, If.configured(spectating=True))
GameplayProtocol.register(SpectateRequest, If.configured(spectating=True))
GameplayProtocol.register(UpdateEvents)
GameplayProtocol.register(UpdatePlayers, If.configured(notice_players=True))
GameplayProtocol.register(UpdateReady)
GameplayProtocol.register(UpdateRequest)


@GameplayProtocol.relation(ALL_PAYLOADS, If.configured(bot=True))
class BotProtocol(Protocol, extends=GameplayProtocol):
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


if __name__ == '__main__':
    players = []
    gamemode = GameMode.CTF
    server_details = ServerDetails(
        client_id=len(players),
        player_id=len(players) + 1,
        level_file_name="battle1.j2l",
        level_crc=1,
        tileset_crc=1,
        game_mode=gamemode,
        max_score=10,
    )

    print(server_details.serialize({}))
