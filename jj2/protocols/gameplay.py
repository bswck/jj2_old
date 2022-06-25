import enum
import functools

from construct import (
    Enum, PaddedString, Byte, Bytes, GreedyBytes, Struct, PascalString, this, Switch, Int,
    Optional, Pass, PrefixedArray, Short, Default, CString, GreedyRange, Int32ul, Int16ul,
    GreedyString, Int8sb, Bitwise, BitsSwapped, Array, OneOf, If
)

from jj2.lib import Payload, AbstractPayload, Protocol

MAIN_ENCODING = 'cp1250'


class ConstructPayload(Payload, has_identity=False):
    struct = Struct()
    has_deferred_data = False

    def _serialize(self, context):
        return self.struct.build(self.data())

    def _deserialize(self, buffer, context):
        return self.struct.parse(buffer)

    def __init_subclass__(cls, compile_struct=not __debug__):
        if compile_struct and cls.struct is not None:
            cls.struct = cls.struct.compile()

    def data(self, for_impl=False):
        if for_impl and self.has_deferred_data:
            return self._data['deferred']
        return self._data


@functools.partial(Enum, PaddedString(4, 'ascii'))
class MajorVersionString(str, enum.Enum):
    v1_23 = '21  '
    v1_24 = '24  '


@functools.partial(Enum, Byte)
class GameMode(enum.IntEnum):
    SINGLEPLAYER = 0
    COOP = 1
    BATTLE = 2
    RACE = 3
    TREASURE_HUNT = 4
    CTF = 5


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


def player_array(with_client_id):
    kwds = {}
    if with_client_id:
        kwds.update(client_id=Byte)
    kwds.update(
        player_id=Byte,
        team=Byte,
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


class GameplayProtocol(Protocol):
    def __init__(self, receiver, sender, **config):
        super().__init__(**config)
        self._deficit = 0
        self._buffer = bytearray()
        self.receiver = receiver
        self.sender = sender

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
            raise ValueError(f'{deficit=} is less than 0')

        self._buffer.extend(data[bof:eof])
        self._deficit = deficit

        if deficit == 0:
            payload = GameplayPayload.from_serialized(buffer=bytes(self._buffer))
            self.handle(payload)
            self._buffer.clear()
            if eof < length:
                tail = data[eof:]
                self.data_received(tail)

    def datagram_received(self, data: bytes):
        payload = GameplayPayload.from_serialized(buffer=bytes(data), checksum=data[:2])
        if payload:
            self.handle(payload)


class GameplayPayload(AbstractPayload, ConstructPayload):
    struct = Struct(
        packet_id=Byte,
        deferred=GreedyBytes
    )

    has_deferred_data = True

    def pick(self, context):
        return self.impls[self._data['packet_id']]

    def serialize(self, context, checksum=False):
        buffer = super().serialize(context)
        if checksum:
            buffer = self.checksum(buffer) + buffer
        return buffer

    def checksum(self, buffer):
        arr = bytes((79, 79)) + buffer
        left = right = 1
        for i in range(2, len(arr)):
            left += arr[i]
            right += left
        return Byte.build(left % 251) + Byte.build(right % 251)

    @classmethod
    def from_serialized(cls, buffer, checksum=None, context=None):
        self = super().from_serialized(buffer=buffer, context=context)
        if checksum is not None:
            if self.checksum() != checksum:
                self = None
        return self


packet_id = GameplayPayload.register


@packet_id(0x03)
class Ping(ConstructPayload):
    struct = Struct(
        number_in_list=Byte,
        unknown_data=Byte[4],
        client_version=MajorVersionString
    )


@packet_id(0x04)
class Pong(ConstructPayload):
    struct = Struct(
        number_in_list_from_ping=Byte,
        unknown_data=Byte[4],
        game_mode_etc=Byte
    )


@packet_id(0x05)
class Query(ConstructPayload):
    struct = Struct(
        number_in_list=Byte
    )


@packet_id(0x06)
class QueryReply(ConstructPayload):
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
class GameEvent(ConstructPayload):
    struct = Struct(
        udp_count=Byte,
        event_id=Enum(Byte, GameEventType),
        event_data=GreedyBytes
    )


@packet_id(0x09)
class Heartbeat(ConstructPayload):
    struct = Struct(
        udp_count=Byte,
        send_back=GreedyBytes,
    )


@packet_id(0x0D)
class ClientDisconnect(ConstructPayload):
    struct = Struct(
        disconnect_message=Enum(Byte, **DISCONNECT_MESSAGES),
        client_id=Int8sb,
        client_version=MajorVersionString,
        include_reason=Optional(Byte),
        reason=If(
            this.include_reason,
            PascalString(Byte, MAIN_ENCODING)
        )
    )


@packet_id(0x10)
class ServerDetails(ConstructPayload):
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
                music_crc=Switch(Byte, [Pass, PrefixedArray(Byte, Byte)]),
                scripts=Switch(
                    Byte, [
                        Pass,
                        Struct(script_crc=Byte[4]),
                        Struct(
                            number_of_script_files=Byte,
                            number_of_required_files=Byte,
                            number_of_optional_files=Byte
                        )
                    ]
                )
            )
        )
    )


@packet_id(0x11)
class NewClient(ConstructPayload):
    struct = Struct(
        client_id=Byte,
        players=PrefixedArray(Byte, player_array(with_client_id=False))
    )


@packet_id(0x12)
class UpdatePlayers(ConstructPayload):
    struct = Struct(
        junk=Byte,
        players=GreedyRange(player_array(with_client_id=True))
    )


@packet_id(0x13)
class GameInit(ConstructPayload):
    struct = Struct()


@packet_id(0x14)
class DownloadingFile(AbstractPayload, ConstructPayload):
    def pick(self, context):
        return self.impls[context.get('is_first', False)]


@DownloadingFile.register(True)
class _DownloadingFileInit(ConstructPayload):
    struct = Struct(
        packet_count=Int32ul,
        unknown_data=Byte[4],
        file_name=PascalString(Byte, MAIN_ENCODING)
    )


@DownloadingFile.register(False)
class _DownloadingFileChunk(ConstructPayload):
    struct = Struct(
        packet_count=Int32ul,
        file_content=GreedyBytes
    )


@packet_id(0x15)
class DownloadRequest(ConstructPayload):
    struct = Struct(file_name=PascalString(Byte, MAIN_ENCODING))


@packet_id(0x16)
class LevelCycled(ConstructPayload):
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
class EndOfLevel(ConstructPayload):
    struct = Struct(unknown_data=GreedyBytes)


@packet_id(0x18)
class UpdateEvents(ConstructPayload):
    struct = Struct(
        checksum=Optional(Short),
        counter=Optional(Short),
        unknown_data=Optional(GreedyString)
    )


@packet_id(0x19)
class ServerStopped(ConstructPayload):  # rip
    struct = Struct()


@packet_id(0x1A)
class UpdateRequest(ConstructPayload):
    struct = Struct(
        level_challenge=Byte[4],
    )


@packet_id(0x1B)
class ChatMessage(ConstructPayload):
    struct = Struct(
        client_id=Byte,
        is_team_chat=Byte,
        text=GreedyString,
    )


@packet_id(0x3F)
class PlusAcknowledgement(ConstructPayload):
    struct = Struct(timestamp=PlusTimestamp)


@packet_id(0x40)
class ConsoleMessage(ConstructPayload):
    struct = Struct(
        message_type=Byte,
        text=GreedyString
    )


@packet_id(0x41)
class Spectate(AbstractPayload, ConstructPayload):
    struct = Struct(
        packet_type=Byte,
        deferred=GreedyBytes
    )
    has_deferred_data = True

    def pick(self, context):
        return self.impls[self._data['packet_type']]


@Spectate.register(0)
class _EachSpectator(ConstructPayload):
    struct = Struct(spectators=Array(4, BitsSwapped(Bitwise(Bytes(8)))))


@Spectate.register(1)
class _Spectators(ConstructPayload):
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
class SpectateRequest(ConstructPayload):
    struct = Struct(
        spectating=OneOf(Byte, (20, 21))
    )

    def feed(self, changes):
        if 'spectating' in changes:
            changes['spectating'] += 20  # (bswck): figure out what this really means
        super().feed(changes)


@packet_id(0x45)
class GameState(ConstructPayload):
    struct = Struct(
        state=Byte,
        time_left=Int
    )


@packet_id(0x49)
class Latency(ConstructPayload):
    struct = Struct(
        latencies=GreedyRange(
            Struct(
                player_id=Byte,
                latency=Short
            )
        )
    )


@packet_id(0x51)
class UpdateReady(ConstructPayload):
    struct = Struct()


@packet_id(0x5A)
class ScriptList(ConstructPayload):
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


GameplayProtocol.register(ChatMessage, read_chat=True)
GameplayProtocol.register(ClientDisconnect, notice_players=True)
GameplayProtocol.register(ConsoleMessage, read_chat=True)
GameplayProtocol.register(DownloadingFile, download_files=True)
GameplayProtocol.register(DownloadRequest, download_files=True)
GameplayProtocol.register(EndOfLevel)
GameplayProtocol.register(GameEvent)
GameplayProtocol.register(GameInit)
GameplayProtocol.register(GameState)
GameplayProtocol.register(Heartbeat)
GameplayProtocol.register(Latency, update_latencies=True)
GameplayProtocol.register(LevelCycled)
GameplayProtocol.register(NewClient, notice_players=True)
GameplayProtocol.register(Ping)
GameplayProtocol.register(PlusAcknowledgement, latest_plus=True)
GameplayProtocol.register(Pong)
GameplayProtocol.register(Query)
GameplayProtocol.register(QueryReply)
GameplayProtocol.register(ScriptList)
GameplayProtocol.register(ServerDetails)
GameplayProtocol.register(ServerStopped)
GameplayProtocol.register(Spectate, spectating=True)
GameplayProtocol.register(SpectateRequest, spectating=True)
GameplayProtocol.register(UpdateEvents)
GameplayProtocol.register(UpdatePlayers, notice_players=True)
GameplayProtocol.register(UpdateReady)
GameplayProtocol.register(UpdateRequest)
