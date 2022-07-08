import abc
import asyncio
import dataclasses
import functools
import random
from typing import Literal

from construct import (
    Byte,
    Bytes,
    GreedyBytes,
    Struct,
    PascalString,
    this,
    Switch,
    Int,
    Optional,
    Pass,
    PrefixedArray,
    Short,
    Default,
    CString,
    GreedyRange,
    Int32ul,
    Int16ul,
    GreedyString,
    Int8sb,
    Bitwise,
    BitsSwapped,
    Array,
    If as ConstructIf,
    BitStruct,
    Padding,
    Flag,
    Renamed,
)
from construct import possiblestringencodings

from jj2.lib import (
    AbstractPayload,
    Protocol,
    If,
    Priority,
    Client,
    Object,
    Property,
    Lazy,
)
from jj2.lib import handles, unformat_jj2_string
from jj2.lib import ALL_PAYLOADS

from jj2.exc import PayloadException
from jj2.constants import *


class ServerProperties(Object):
    allows_fireball = Property(False)
    allows_mouse_aim = Property(False)
    allows_ready = Property(True)
    allows_walljump = Property(True)
    allows_running = Property(True)

    auto_weapon_update = Property(True)
    colour_depth = Property()
    friendly_fire = Property(False)
    is_tsf = Property(False)
    key_chat = Property()
    low_detail = Property(False)
    max_score = Property(10)
    mouse_aim = Property(False)

    music_active = Property()
    music_volume = Property()

    no_blink = Property(False)
    max_objects = Property()
    plus_version = Property((5, 9))

    resolution_max_height = Property(0)
    resolution_max_width = Property(0)
    quirks = Property(True)
    show_max_health = Property(False)
    snowing_intensity = Property()
    sound_enabled = Property()
    start_health = Property()
    strong_powerups = Property(True)


class Session(Object):
    server = Property.object(ServerProperties)
    plus_version = Property((5, 9))
    udp_source_port = Property(0)
    client_id = Property()
    from_server = Property(True)
    introduced = Property(False)

    players = Property([], collection=True)

    client_version = Property((1, 24))

    local_players = Property([], collection=True)
    number_of_local_players = Lazy(local_players)

    @local_players.on_get
    def _on_local_players(self, local_players):
        for local_player_id, local_player in enumerate(local_players, start=1):
            local_player.player_id = local_player_id

    @number_of_local_players.mapper
    def _number_of_local_players(self, local_players):
        return len(local_players)

    level_challenge = Property([0, 0, 0, 0])
    heartbeat_latency = Property(0)
    heartbeat_cookie = Property([0, 0, 0, 0])

    backup_palette = Property(None)
    border_height = Property(0)
    border_width = Property(0)

    bottom_feeder = Property(0)

    deactivating_because_of_death = Property(False)
    delay_generated_crate_origins = Property(False)
    difficulty = Property(0)
    do_zombies_already_exist = Property(False)
    echo = Property(0)

    connection = Property(GAME.CONNECTION.ONLINE)
    custom = Property(GAME.CUSTOM.NOCUSTOM)
    state = Property(GAME.STATE.STOPPED)

    is_admin = Property(False)
    is_server = Property(False)
    is_snowing = Property(False)
    is_snowing_outdoors_only = Property(False)

    level_file_name = Property()
    level_name = Property()
    music_file_name = Property()
    scripts = Property([], collection=True)

    palette = Property(None)

    render_frame = Property(0)

    resolution_height = Property(0)
    resolution_width = Property(0)

    snowing_type = Property(None)
    subscreen_height = Property()
    subscreen_width = Property()
    sugar_rush_allowed = Property(False)

    textured_bg_fade_position_x = Property()
    textured_bg_fade_position_y = Property()
    textured_bg_stars = Property()
    textured_bg_style = Property()
    textured_bg_texture = Property()
    textured_bg_used = Property()

    water_interaction = Property()
    water_layer = Property()
    water_lighting = Property()
    water_target = Property()


class Resource(Object):
    session = Property.object(Session)
    filename = Property()
    unknown_data = Property()


@dataclasses.dataclass
class Fur:
    body_colour: int = 16
    stirnband_colour: int = 24
    blaster_colour: int = 32
    wristband_colour: int = 40
    body_1_colour: int = 24
    shoes_and_wristband_colour: int = 32
    body_2_colour: int = 40
    blaster_1_colour: int = 24
    blaster_2_colour: int = 32

    def code_for(self, character):
        if character in (CHARACTER.JAZZ, CHARACTER.BIRD, CHARACTER.BIRD2):
            return (
                self.body_colour,
                self.stirnband_colour,
                self.blaster_colour,
                self.wristband_colour,
            )
        if character == CHARACTER.SPAZ:
            return (
                self.blaster_colour,
                self.body_1_colour,
                self.shoes_and_wristband_colour,
                self.body_2_colour,
            )
        if character in (CHARACTER.LORI, CHARACTER.FROG):
            return 0, self.blaster_1_colour, self.blaster_2_colour, self.body_colour
        return 16, 24, 32, 40

    @classmethod
    def from_code(cls, colour_1, colour_2, colour_3, colour_4):
        return cls(
            body_colour=colour_1,
            stirnband_colour=colour_2,
            blaster_colour=colour_3,
            wristband_colour=colour_4,
            body_1_colour=colour_2,
            shoes_and_wristband_colour=colour_3,
            body_2_colour=colour_4,
            blaster_1_colour=colour_2,
            blaster_2_colour=colour_3,
        )


class Rabbit:
    def __init__(self, name=None, team=TEAM.BLUE, character=CHARACTER.JAZZ, fur=Fur()):
        self._name = None
        self._name_unformatted = None
        self.team = team
        self.character = character
        self._fur = fur

        self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name
        if name:
            self._name_unformatted = unformat_jj2_string(name)

    @property
    def name_unformatted(self):
        return self._name_unformatted

    @property
    def fur(self):
        return self._fur.code_for(self.character)


class Player(Object):
    rabbit = Property(Rabbit())
    team = Lazy(rabbit.team)
    character = Lazy(rabbit.character)
    fur_colour = Lazy(rabbit.fur)
    rabbit_name = Lazy(rabbit.name)
    rabbit_name_unformatted = Lazy(rabbit.name_unformatted)

    session = Property.object(Session)
    client_id = Lazy(session.client_id)
    player_id = Property()

    ammo = Property(dict.fromkeys(WEAPON._value2member_map_, 0))
    anti_grav = Property(False)
    ball_time = Property(0)
    blink = Property(0)
    boss = Property()
    boss_activated = Property(False)
    buttstomp = Property()

    camera_x = Property(0.0)
    camera_y = Property(0.0)

    char_curr = Property()
    char_orig = Property()

    coins = Property(0)
    cur_anim = Property()
    cur_frame = Property()
    curr_tile = Property()
    curr_weapon = Property()
    deaths = Property(0)

    direction = Property()
    double_jump_count = Property(0)

    fastfire = Property(35)
    flag = Property(0)
    fly = Property()
    food = Property()
    frame_id = Property()
    frozen = Property(0)
    gems = Property(dict.fromkeys(GEM._value2member_map_, 0))
    health = Property()
    helicopter = Property()
    helicopter_elapsed = Property()
    idle = Property(0)

    invincibility = Property()
    invinsibility = Property()

    is_active = Property()
    is_admin = Property()
    is_connecting = Property()
    is_idle = Property()
    is_in_game = Property()
    is_jailed = Property()
    is_local = Property()
    is_out = Property()
    is_spectating = Property()
    is_zombie = Property()

    jump_strength = Property(-10)

    key_down = Property()
    key_fire = Property()
    key_jump = Property()
    key_left = Property()
    key_right = Property()
    key_run = Property()
    key_select = Property()
    key_up = Property()

    laps = Property()
    lap_time_best = Property()
    lap_time_current = Property()
    lap_times = Property([-1, -1, -1, -1, -1])

    light = Property()
    lighting = Property()
    light_type = Property(LIGHT.PLAYER)
    lives = Property()

    local_player_id = Property(0)

    lrs_lives = Property()

    noclip_mode = Property()
    nofire = Property()
    platform = Property(0)
    id = Property()
    powerup = Property(dict.fromkeys(WEAPON._value2member_map_, False))
    roasts = Property(0)
    running = Property(False)
    score = Property(0)

    anim_set_id = Property()

    shield_time = Property()
    shield_type = Property(SHIELD.NONE)

    special_move = Property(0)
    sprite_mode = Property(SPRITEMODE.BLEND_NORMAL)
    sprite_param = Property()
    stoned = Property()

    subscreen_x = Property(0)
    subscreen_y = Property(0)

    timer_persists = Property()
    timer_state = Property(TIMER.STOPPED)
    timer_time = Property()

    warp_id = Property(0)

    x_acc = Property(0.0)
    x_org = Property(0.0)
    x_pos = Property(0.0)
    x_speed = Property(0.0)
    y_acc = Property(0.0)
    y_org = Property(0.0)
    y_pos = Property(0.0)
    y_speed = Property(0.0)

    def to_payload_data(self, payload_cls):
        if payload_cls is ClientDetails:
            return dict(
                player_id=self.player_id,
                team=self.team,
                character=self.character,
                fur_colour=self.fur_colour,
                sprite_mode_param=10,
                unused=124,
                rabbit_name=self.rabbit_name,
            )
        return self


MAIN_ENCODING = "cp1250"
possiblestringencodings[MAIN_ENCODING] = 1


def _cast_to_data(payload_cls, obj):
    if isinstance(obj, (tuple, list)):
        obj = type(obj)(map(functools.partial(_cast_to_data, payload_cls), obj))
    elif hasattr(obj, "to_payload_data"):
        obj = obj.to_payload_data(payload_cls)
    return obj


def _collect_by_struct(payload_cls, subcons, data):
    result = {}
    if data is None:
        return result
    for subcon in subcons:
        value = _cast_to_data(payload_cls, data.get(subcon.name))
        if (
            value is None
            and isinstance(subcon, Default)
            or (isinstance(subcon, Renamed) and isinstance(subcon.subcon, Default))
        ):
            continue
        if isinstance(subcon, Struct):
            value = _collect_by_struct(payload_cls, subcon.subcons, value)
        result[subcon.name] = value
    return result


def _struct_parsed(container, _):
    container.pop("_io", None)


def struct(*args, **kwargs):
    obj = Struct(*args, **kwargs)
    obj.parsed = _struct_parsed
    return obj


class BinaryPayload(AbstractPayload, abc.ABC, has_feed=False):
    struct = struct(buffer=GreedyBytes)
    feeds = "buffer"
    has_default_implementation = True

    def _serialize(self, context, **kwargs):
        try:
            return self.struct.build(self.data(deserialization=False), **kwargs)
        except Exception as exc:
            raise PayloadException(self.event or "unknown event") from exc

    def _deserialize(self, serialized, context, **kwargs):
        try:
            return self.struct.parse(serialized, **kwargs)
        except Exception as exc:
            raise PayloadException(self.event or "unknown event") from exc

    @classmethod
    def from_dict(cls, data):
        return cls(**_collect_by_struct(cls, cls.struct.subcons, data))

    def __init_subclass__(cls, compiled_structs=False, feeds=None):
        if compiled_structs and cls.struct is not None:
            cls.struct = cls.struct.compile()
        if feeds:
            cls.feeds = feeds
        super().__init_subclass__()


class GamePayload(BinaryPayload):
    struct = struct(packet_id=Byte, buffer=GreedyBytes)
    has_default_implementation = False

    def _get_impl_key(self, context):
        return self._data["packet_id"]

    def _set_impl_key(self, key, context):
        self._data["packet_id"] = key

    def _impl_data(self, deserialization=False):
        return self._data["buffer"]

    def serialize(self, context=None, checksum=False, **kwargs) -> bytes:
        if self.serialized is None:
            serialized = super().serialize(context, **kwargs)
            if checksum:
                serialized = self.checksum(serialized) + serialized
            self.serialized = serialized
        return self.serialized

    @staticmethod
    def checksum(serialized):
        arr = bytes((79, 79)) + serialized
        lsb = msb = 1
        for i in range(2, len(arr)):
            lsb += arr[i]
            msb += lsb
        return Byte.build(lsb % 251) + Byte.build(msb % 251)

    @classmethod
    def load(cls, serialized, context=None, checksum=None, **options):
        if checksum is not None:
            if cls.checksum(serialized) != checksum:
                self = None
            serialized = serialized[2:]
        self = super().load(serialized=serialized, context=context, **options)
        return self


packet_id = GamePayload.register


@packet_id(0x03)
class Ping(BinaryPayload):
    event = "ping"
    struct = struct(number_in_list=Byte, unknown_data=Byte[4], client_version=Byte[4])


@packet_id(0x04)
class Pong(BinaryPayload):
    event = "pong"
    struct = struct(
        number_in_list_from_ping=Byte, unknown_data=Byte[4], game_mode_etc=Byte
    )


@packet_id(0x05)
class Query(BinaryPayload):
    event = "query"
    struct = struct(number_in_list=Byte)


@packet_id(0x06)
class QueryReply(BinaryPayload):
    event = "query_reply"
    struct = struct(
        number_in_list=Byte,
        timer_sync=Byte,
        laps_on_timer_sync=Byte,
        unknown_data_1=Byte[2],
        client_version=BIN_VERSIONSTRING,
        player_count=Byte,
        unknown_data_2=Byte,
        game_mode=BIN_GAMEMODE,
        player_limit=Byte,
        server_name=PascalString(Byte, MAIN_ENCODING),
        unknown_data_3=Byte,
    )


@packet_id(0x07)
class GameEvent(BinaryPayload):
    event = "game_event"
    struct = struct(udp_count=Byte, event_id=BIN_GAMEEVENT, event_data=GreedyBytes)


@packet_id(0x09)
class Heartbeat(BinaryPayload):
    event = "heartbeat"
    struct = struct(
        heartbeat_latency=Byte,
        heartbeat_cookie=Default(GreedyBytes, b""),
    )


# @packet_id(0x0A)
class Password(BinaryPayload):
    event = "password"
    struct = struct(password=PascalString(Byte, MAIN_ENCODING))


# @packet_id(0x0B)
class PasswordCheck(BinaryPayload):
    event = "password_check"
    struct = struct(password_ok=Byte)


@packet_id(0x0D)
class ClientDisconnect(BinaryPayload):
    event = "disconnect"
    struct = struct(
        disconnect_message=BIN_DISCONNECTMESSAGE,
        client_id=Int8sb,
        client_version=BIN_VERSIONSTRING,
        include_reason=Optional(Flag),
        reason=ConstructIf(this.include_reason, PascalString(Byte, MAIN_ENCODING)),
    )


def player_array(*, client_id):
    subcons = {}
    if client_id:
        subcons.update(client_id=Byte)
    subcons.update(
        player_id=Byte,
        team=Default(BIN_TEAM, TEAM.BLUE.value),
        character=Default(BIN_CHAR, CHARACTER.SPAZ.value),
        fur_colour=Byte[4],
        sprite_mode=Default(Byte, 1),
        sprite_mode_param=Default(Byte, this.player_id),
        light_type=Default(Byte, 13),
        light_size=Default(Byte, 0),
        antigrav_and_nofire=Default(Byte, 0),
        unused=Default(Byte, 0),
        rabbit_name=CString(MAIN_ENCODING),
    )
    return struct(**subcons)


@packet_id(0x0E)
class ClientDetails(BinaryPayload):
    event = "client_details"
    struct = struct(
        client_id=Byte, local_players=PrefixedArray(Byte, player_array(client_id=False))
    )


@packet_id(0x0F)
class JoinRequest(BinaryPayload):
    event = "join_request"
    struct = struct(
        udp_source_port=Int16ul,
        client_version=BIN_VERSIONSTRING,
        number_of_local_players=Byte,
    )


@packet_id(0x10)
class ServerDetails(BinaryPayload):
    event = "server_details"
    struct = struct(
        client_id=Byte,
        unknown=Byte,
        level_file_name=PascalString(Byte, MAIN_ENCODING),
        level_crc=Int,
        tileset_crc=Int,
        game_mode=BIN_GAMEMODE,
        max_score=Byte,
        extras=Optional(
            struct(
                level_challenge=Byte[4],
                heartbeat_cookie=Byte[4],
                plus_version=Short[2],
                music_crc=Switch(
                    Byte, dict(enumerate([Pass, PrefixedArray(Byte, Byte)]))
                ),
                scripts=Switch(
                    Byte,
                    dict(
                        map(
                            tuple,
                            enumerate(
                                [
                                    Pass,
                                    struct(script_crc=Byte[4]),
                                    struct(
                                        number_of_script_files=Byte,
                                        number_of_required_files=Byte,
                                        number_of_optional_files=Byte,
                                    ),
                                ]
                            ),
                        )
                    ),
                ),
            )
        ),
    )


@packet_id(0x12)
class PlayerList(BinaryPayload):
    event = "player_list"
    struct = struct(
        number_of_players=Byte,  # byte unsure, omit it
        players=GreedyRange(player_array(client_id=True)),
    )


@packet_id(0x13)
class GameInit(BinaryPayload):
    event = "game_init"
    struct = struct()


@packet_id(0x14)
class DownloadingFile(BinaryPayload):
    event = "downloading_file"

    def _get_impl_key(self, context):
        return context.get("is_downloading", False)


@DownloadingFile.register(False)
class _DownloadingFileInit(BinaryPayload):
    event = "downloading_file"
    struct = struct(
        packet_count=Int32ul,
        unknown_data=Byte[4],
        file_name=PascalString(Byte, MAIN_ENCODING),
    )


@DownloadingFile.register(True)
class _DownloadingFileChunk(BinaryPayload):
    event = "downloading_file"
    struct = struct(packet_count=Int32ul, file_content=GreedyBytes)


@packet_id(0x15)
class DownloadRequest(BinaryPayload):
    event = "download_request"
    struct = struct(file_name=PascalString(Byte, MAIN_ENCODING))


@packet_id(0x16)
class LevelLoad(BinaryPayload):
    event = "level_load"
    struct = struct(
        level_crc=Int,
        tileset_crc=Int,
        level_file_name=PascalString(Byte, MAIN_ENCODING),
        level_challenge=Byte[4],
        is_different=Flag,
        music=Byte,
        music_crc=Byte[4],
        script_data=Optional(Byte[5]),
    )


@packet_id(0x17)
class EndOfLevel(BinaryPayload):
    event = "end_of_level"
    struct = struct(unknown_data=GreedyBytes)


@packet_id(0x18)
class UpdateEvents(BinaryPayload):
    event = "update_events"
    struct = struct(
        checksum=Optional(Short),
        counter=Optional(Short),
        unknown_data=Optional(GreedyString(MAIN_ENCODING)),
    )


@packet_id(0x19)
class ServerStopped(BinaryPayload):  # rip
    event = "stopped"
    struct = struct()


@packet_id(0x1A)
class UpdateRequest(BinaryPayload):
    event = "update_request"
    struct = struct(
        level_challenge=Byte[4],
    )


@packet_id(0x1B)
class ChatMessage(BinaryPayload):
    event = "chat"
    struct = struct(
        client_id=Byte,
        chat_type=BIN_CHAT,
        text=GreedyString(MAIN_ENCODING),
    )


@packet_id(0x3F)
class PlusAcknowledgement(BinaryPayload):
    event = "plus"

    def _get_impl_key(self, context):
        return context.get("from_server", True)

    def _impl_data(self, deserialization=False):
        return self._data["buffer"]


@PlusAcknowledgement.register(False)  # client-side
class PlusRequest(BinaryPayload):
    event = "plus_request"
    struct = struct(plus_version=BIN_PLUSTIMESTAMP)


@PlusAcknowledgement.register(True)  # server-side
class PlusDetails(BinaryPayload):
    event = "plus_details"
    struct = struct(
        unknown=Byte,
        health_info=Byte,
        plus_data=BitStruct(
            pad=Padding(4),
            no_blink=Flag,
            no_movement=Flag,
            friendly_fire=Flag,
            plus_only=Flag,
        ),
    )


@packet_id(0x40)
class ConsoleMessage(BinaryPayload):
    event = "console"
    struct = struct(
        message_type=Byte,
        content=GreedyString(MAIN_ENCODING)
        # content=IfThenElse(
        #     this.message_type == 4,
        #     struct(message=PaddedString(this.length - 5, MAIN_ENCODING), parameters=Bytes(2)),
        #     struct(message=GreedyString(MAIN_ENCODING)),
        # )
    )


@packet_id(0x41)
class Spectate(BinaryPayload):
    event = "spectate_pkt"
    struct = struct(packet_type=Byte, buffer=GreedyBytes)

    def _get_impl_key(self, context):
        return self._data["packet_type"]

    def _set_impl_key(self, key, context):
        self._data["packet_type"] = key

    def _impl_data(self, deserialization=False):
        return self._data["buffer"]


@Spectate.register(0)
class _SpectatorList(BinaryPayload):
    event = "spectator_list"
    struct = struct(spectators=Bitwise(Array(8, BitsSwapped(Bytes(4)))))


@Spectate.register(1)
class _Spectators(BinaryPayload):
    event = "spectators"
    struct = struct(
        spectators=GreedyRange(
            struct(is_out=Flag, client_id=Int8sb, spectate_target=BIN_SPECTATETARGET)
        )
    )


@packet_id(0x42)
class SpectateRequest(BinaryPayload):
    event = "spectate"
    struct = struct(
        spectating=Byte,
    )

    def feed(self, changes):
        if "spectating" in changes:
            changes["spectating"] = 20 + (changes["spectating"] % 2)
        super().feed(changes)


@packet_id(0x45)
class GameState(BinaryPayload):
    event = "game_state"
    struct = struct(
        state=BitStruct(pad=Padding(5), in_overtime=Bytes(2), game_started=Flag),
        time_left=Int,
    )


@packet_id(0x49)
class Latency(BinaryPayload):
    event = "latencies"
    struct = struct(latencies=GreedyRange(struct(player_id=Byte, latency=Short)))

    def data(self, deserialization=True, to_impl=False):
        data = super().data(deserialization, to_impl)
        if deserialization:
            for latency_details in data["latencies"]:
                latency_details["latency"] >>= 8
        return data


@packet_id(0x51)
class Ready(BinaryPayload):
    event = "ready"
    struct = struct()


@packet_id(0x5A)
class ResourceList(BinaryPayload):
    event = "scripts"
    struct = struct(
        level_challenge=Byte[4],
        script_data=Byte[5],
        scripts=GreedyRange(
            struct(
                unknown_data=Byte[5],
                filename=PascalString(Byte, MAIN_ENCODING),
            )
        ),
    )


def _setup_registrar_ip(registered, ip):
    registered.ip = ip
    for impl in registered.impls.values():
        _setup_registrar_ip(impl, ip)


class GameProtocol(Protocol, asyncio.Protocol, asyncio.DatagramProtocol):
    payload_cls = GamePayload

    def __init__(
        self,
        protocol=None,
        *,
        engine,
        future,
        bot=True,
        capture_packets=True,
        from_server=True,
        chat=True,
        notice_players=True,
        download_files=True,
        passwords=True,
        spectating=True,
        update_latencies=True,
        **config,
    ):
        super().__init__(
            protocol,
            bot=bot,
            capture_packets=capture_packets,
            from_server=from_server,
            chat=chat,
            notice_players=notice_players,
            download_files=download_files,
            passwords=passwords,
            spectating=spectating,
            update_latencies=update_latencies,
            **config,
        )
        self.engine = engine
        self.session = Session(self)
        self._future: asyncio.Future = future
        self._deficit = 0
        self._buffer = bytearray()
        self._tcp_transport = None
        self._udp_transport = None
        self._udp_addr = None

    @property
    def future(self):
        return self._future

    def connection_made(self, transport):
        if hasattr(transport, "sendto"):
            self._udp_transport = transport
            self._udp_addr, source_port = transport.get_extra_info("sockname", 0)
            self.session.udp_source_port = source_port
        else:
            self._tcp_transport = transport

    def connection_lost(self, exc=None) -> None:
        self._tcp_transport = None
        self._udp_transport = None
        self.future.cancel()
        self.session.introduced = False

    def send(self, data: bytes):
        length = len(data) + 1
        arr = bytearray()
        if length > 255:
            length += 2
            lsb = Int16ul.build(length)
            arr.append(0)
        else:
            lsb = length
        arr.append(lsb)
        arr.extend(data)
        return self._tcp_transport.write(arr)

    def sendto(self, data: bytes):
        return self._udp_transport.sendto(data)

    def submit(self, payload):
        ip = payload.ip.lower()
        if ip == "tcp":
            self.send(payload.serialize(context=self.session))
        if ip == "udp":
            self.sendto(payload.serialize(context=self.session, checksum=True))
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
            raise ValueError(f"{deficit=} < 0")

        self._buffer.extend(data[bof:eof])
        self._deficit = deficit

        if deficit == 0:
            self.handle_data(bytes(self._buffer), context=self.session)
            self._buffer.clear()
            if eof < length:
                tail = data[eof:]
                self.data_received(tail)

    def datagram_received(self, data: bytes, addr: tuple):
        self.handle_data(data, context=self.session, checksum=data[:2])

    def eof_received(self):
        self.engine.dispatch(self, "eof")
        return False

    def error_received(self, exc):
        super().on_error(msg=f"send/receive operation of UDP ({exc})")

    @classmethod
    def register(
        cls, registered=None, condition=None, *, ip: Literal["tcp", "udp"] = None
    ):
        if registered is None:
            return functools.partial(cls.register, registered, condition, ip=ip)
        if ip is None:
            raise ValueError("ip (internet protocol) must be either TCP or UDP")
        registered = super().register(registered, condition)
        _setup_registrar_ip(registered, ip)
        return registered

    @handles(ServerDetails, priority=Priority.URGENT)
    def on_server_details(self, payload: ServerDetails):
        data = payload.data()
        self.session.client_id = data["client_id"]

        extras = data.get("extras")
        if extras:
            self.session.level_challenge = extras["level_challenge"]
            self.session.heartbeat_cookie = extras["heartbeat_cookie"]

    @handles(ResourceList, priority=Priority.URGENT)
    def on_script_list(self, payload: ResourceList):
        data = payload.data()
        self.session.level_challenge = data["level_challenge"]
        self.session.scripts = [
            Resource(**script_data) for script_data in data["scripts"]
        ]

    @handles(ClientDisconnect, priority=Priority.URGENT)
    def on_client_disconnect(self, payload: ClientDisconnect):
        data = payload.data()
        if data["client_id"] == -1:
            self.connection_lost()

    @handles(Heartbeat, priority=Priority.URGENT)
    def on_heartbeat(self, payload: Heartbeat):
        data = payload.data()
        latency = data["heartbeat_latency"]
        self.session.heartbeat_latency = min(
            (random.randint(latency + 1, latency + 20), 255)
        )
        self.session.heartbeat_cookie = list(data["heartbeat_cookie"])

    @handles(LevelLoad, priority=Priority.URGENT)
    def on_level_load(self, payload: LevelLoad):
        data = payload.data()
        self.session.level_file_name = data["level_file_name"]
        self.session.level_challenge = data["level_challenge"]

    @handles(ALL_PAYLOADS, priority=Priority.NORMAL)
    def dispatch(self, payload):
        self.engine.dispatch(self, payload)


GameProtocol.register(ChatMessage, If.configured(chat=True), ip="tcp")
GameProtocol.register(ClientDetails, If.configured(notice_players=True), ip="tcp")
GameProtocol.register(ClientDisconnect, If.configured(notice_players=True), ip="tcp")
GameProtocol.register(ConsoleMessage, If.configured(chat=True), ip="tcp")
GameProtocol.register(DownloadingFile, If.configured(download_files=True), ip="tcp")
GameProtocol.register(DownloadRequest, If.configured(download_files=True), ip="tcp")
GameProtocol.register(EndOfLevel, ip="tcp")
GameProtocol.register(GameEvent, ip="udp")
GameProtocol.register(GameInit, ip="tcp")
GameProtocol.register(GameState, ip="udp")
GameProtocol.register(Heartbeat, ip="udp")
GameProtocol.register(PlayerList, ip="tcp")
GameProtocol.register(JoinRequest, ip="tcp")
GameProtocol.register(Latency, If.configured(update_latencies=True), ip="tcp")
GameProtocol.register(LevelLoad, ip="tcp")
# GameProtocol.register(Password, If.configured(passwords=True), ip='udp')
# GameProtocol.register(PasswordCheck, If.configured(passwords=True), ip='udp')
GameProtocol.register(Ping, ip="udp")
GameProtocol.register(PlusAcknowledgement, If.configured(latest_plus=True), ip="tcp")
GameProtocol.register(Pong, ip="udp")
GameProtocol.register(Query, ip="udp")
GameProtocol.register(QueryReply, ip="udp")
GameProtocol.register(ResourceList, ip="tcp")
GameProtocol.register(ServerDetails, ip="tcp")
GameProtocol.register(ServerStopped, ip="tcp")
GameProtocol.register(Spectate, If.configured(spectating=True), ip="tcp")
GameProtocol.register(SpectateRequest, If.configured(spectating=True), ip="tcp")
GameProtocol.register(UpdateEvents, ip="tcp")
GameProtocol.register(PlayerList, If.configured(notice_players=True), ip="tcp")
GameProtocol.register(Ready, ip="tcp")
GameProtocol.register(UpdateRequest, ip="tcp")


@GameProtocol.handles(ALL_PAYLOADS, If.configured(bot=True))
class BotProtocol(Protocol, extends=GameProtocol):
    """Packet coordination in the background using default bot behavior."""

    def __init__(
        self, protocol=None, *, join_servers=True, autospectate=True, **config
    ):
        if protocol is None:
            raise ValueError(
                "the bot protocol relies on running instance of the gameplay protocol"
            )
        super().__init__(
            protocol, join_servers=join_servers, autospectate=autospectate, **config
        )

    @property
    def session(self):
        return self.protocol.session

    @handles(ServerDetails, priority=Priority.IMPORTANT)
    def on_server_details(self, _):
        self.submit_all(
            Heartbeat(heartbeat_latency=0), PlusRequest.from_dict(self.session)
        )

    @handles(Heartbeat, priority=Priority.IMPORTANT)
    @handles(ResourceList, priority=Priority.IMPORTANT)
    def on_script_list(self, _):
        self.submit(
            Heartbeat(
                heartbeat_latency=self.session.get("heartbeat_latency") or 1,
                heartbeat_cookie=bytes(self.session.get("heartbeat_cookie", 0)),
            )
        )

    @handles(PlusDetails, If.configured(join_servers=True), priority=Priority.IMPORTANT)
    def on_plus_details(self, _):
        if not self.session.introduced:
            self.submit(ClientDetails.from_dict(self.session))
            self.session.introduced = True

    @handles(LevelLoad, priority=Priority.IMPORTANT)
    @handles(Ready, priority=Priority.IMPORTANT)
    @handles(GameInit, priority=Priority.IMPORTANT)
    def on_ready(self, _):
        self.submit(UpdateRequest.from_dict(self.session))


class GameClient(Client):
    def __init__(self, local_players, **config):
        config["from_server"] = True
        config.setdefault("bot", True)
        super().__init__(**config)
        self.local_players = local_players

    async def run(self, timeout=None):
        await super().run(timeout=timeout)
        futs = [proto.future for protos in self.protocols.values() for proto in protos]
        if futs:
            await asyncio.wait(futs, timeout=timeout)

    async def connect(self, host, port=10052) -> GameProtocol:
        protocol = GameProtocol(
            engine=self, future=self.loop.create_future(), **self.config
        )
        protocol.session.local_players = self.local_players
        await self.loop.create_connection(lambda: protocol, host=host, port=port)
        await self.loop.create_datagram_endpoint(
            lambda: protocol, remote_addr=(host, port)
        )
        return self.register_protocol((host, port), protocol)
