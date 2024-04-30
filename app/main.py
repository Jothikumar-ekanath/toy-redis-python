import asyncio
from app.parser import RESPParser, DataType, Constant, Command
import argparse

# to store Key-Value pairs
cache = {}
args = None
# replication data
replication = {
    'role': 'master',
    'connected_slaves': 0,
    'master_replid': '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
    'master_repl_offset': 0
}
# coroutine that will start another coroutine after a delay in seconds


async def delay(coro, seconds):
    # suspend for a time limit in seconds
    await asyncio.sleep(seconds)
    # execute the other coroutine
    await coro


async def pop_cache(key: str) -> None:
    print(f"Expiring key {key}")
    cache.pop(key, None)


async def encode(datatype: DataType, data: bytes | list[bytes]):
    """
    Encode data as per RESP specifications
    """

    if datatype in (DataType.SIMPLE_STRING, DataType.SIMPLE_ERROR):
        return Constant.TERMINATOR.join([datatype + data, Constant.EMPTY_BYTE])

    if datatype == DataType.BULK_STRING:
        length = len(data)
        return Constant.TERMINATOR.join([datatype + str(length).encode(), data, Constant.EMPTY_BYTE])

    if datatype == DataType.ARRAY:
        num_elements = len(data)
        return Constant.TERMINATOR.join([
            datatype +
            str(num_elements).encode(), Constant.EMPTY_BYTE.join(data)
        ])


async def execute_resp_commands(commands: list[str] | None) -> bytes:
    if not commands:
        return await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)
    else:  # Treat all other RESP commands
        cmd = commands[0].lower()
        match cmd:
            case Command.PING:
                return await encode(DataType.SIMPLE_STRING, Constant.PONG)
            case Command.ECHO:
                return await encode(DataType.SIMPLE_STRING, commands[1].encode())
            case Command.GET:
                # Example: handling GET command
                key = commands[1]
                value = cache.get(key,None)
                if value is None:
                    return Constant.NULL_BULK_STRING
                return await encode(DataType.BULK_STRING, value.encode())
            case Command.SET:
                # Example: handling SET command
                key = commands[1]
                value = commands[2]
                cache[key] = value
                if len(commands) > 3 and commands[3].upper() == "PX":
                    # SET key value PX milliseconds
                    delay_sec = int(commands[4]) / 1000
                    # print(f"Setting key {key} to expire in {delay_sec} seconds")
                    asyncio.create_task(delay(pop_cache(key), delay_sec))
                return await encode(DataType.SIMPLE_STRING, Constant.OK)
            case Command.INFO:
                data = '\n'.join(
                    [f'{key}:{value}' for key, value in replication.items()]).encode()
                return await encode(DataType.BULK_STRING, data)
            case Command.REPLCONF:
                return await encode(DataType.SIMPLE_STRING, Constant.OK)
            case Command.PSYNC:
                return await encode(
                DataType.SIMPLE_STRING, 
                Constant.SPACE_BYTE.join([
                    Constant.FULLRESYNC, 
                    replication['master_replid'].encode(), 
                    str(replication['master_repl_offset']).encode()
                ]))
    return await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)


async def connection_handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while not reader.at_eof():
            addr = writer.get_extra_info("peername")
            commands = await RESPParser.parse_resp_request(reader)
            print(f"RESP parsed_req: {commands}")
            response = await execute_resp_commands(commands)
            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"An error occurred for {addr}: {e}")
    finally:
        print(f"Closing the connection with {addr}")
        #writer.close()
        await writer.wait_closed()


async def send_handshake_replica(address):
    host, port = address
    global args
    reader, writer = await asyncio.open_connection(host, port)

    # sends PING command
    writer.write(b'*1\r\n$4\r\nping\r\n')
    await writer.drain()
    response = await reader.readuntil(Constant.TERMINATOR)
    print(f"Received handshake PING response: {response}")
    # sends REPLCONF listening-port <PORT> command
    writer.write(
        await encode(DataType.ARRAY, [
            await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
            await encode(DataType.BULK_STRING, 'listening-port'.encode()),
            await encode(DataType.BULK_STRING, str(args.port).encode())
        ]))
    await writer.drain()
    response = await reader.readuntil(Constant.TERMINATOR)
    print(
        f"Received handshake REPLCONF listening-port <PORT> response: {response}")

    writer.write(
        await encode(DataType.ARRAY, [
            await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
            await encode(DataType.BULK_STRING, 'capa'.encode()),
            await encode(DataType.BULK_STRING, 'psync2'.encode())
        ]))
    await writer.drain()
    response = await reader.readuntil(Constant.TERMINATOR)
    print(f"Received handshake REPLCONF capa psync2 response: {response}")
    # sends PSYNC ? -1 command
    writer.write(
    await encode(DataType.ARRAY, [
        await encode(DataType.BULK_STRING, Command.PSYNC.encode()),
        await encode(DataType.BULK_STRING, '?'.encode()),
        await encode(DataType.BULK_STRING, '-1'.encode())
    ]))
    await writer.drain()
    response = await reader.readuntil(Constant.TERMINATOR)
    print(f"Received handshake PSYNC response: {response}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)
    parser.add_argument('--replicaof', nargs=2, type=str)
    global args
    args = parser.parse_args()
    if args.replicaof:
        replication['role'] = 'slave'
        await send_handshake_replica(args.replicaof)

    server = await asyncio.start_server(connection_handler, "localhost", args.port or 6379)
    print(f"Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
