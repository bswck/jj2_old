from jj2 import Priority, GameClient, Player, GameProtocol, JoinRequest, ALL_PAYLOADS, Rabbit, \
    GamePayload, GameEvent, Heartbeat

client = GameClient(local_players=[Player(rabbit=Rabbit('ak'))])


@client.setup
async def setup():
    protocol = await client.connect('127.0.0.1')
    protocol.submit(JoinRequest.from_dict(protocol.session))


if __name__ == '__main__':
    client.start()

