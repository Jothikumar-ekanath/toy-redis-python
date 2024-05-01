import asyncio
from app.parser import RESPParser, DataType, Constant, Command
import argparse
import traceback
# to store Key-Value pairs
cache = {}
replica_connections = []
write_commands = set([Command.SET])
rdb_state = bytes.fromhex(
    '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')
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
                value = cache.get(key, None)
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
        addr = writer.get_extra_info('peername')
        while True:
            print(f"{replication['role']} - Waiting for the next command...{addr}")
            commands = await RESPParser.parse_resp_request(reader)
            if commands is None:
                print(f"{replication['role']} - Received empty byte from client, assuming Connection closed {addr}")
                break
            print(f"{replication['role']} - RESP parsed_req: {commands} for client {addr}")
            # Collecting the replica connections
            if commands and commands[0].lower() == Command.REPLCONF and commands[1] == 'listening-port':
                replica_connections.append((reader, writer))
                print(f"{replication['role']} - Replica connections added for {addr}")
            response = await execute_resp_commands(commands)
            writer.write(response)
            await writer.drain()
            # Sending the empty RDB file
            if commands and commands[0].lower() == Command.PSYNC:
                writer.write(Constant.EMPTY_BYTE.join([DataType.BULK_STRING, str(
                    len(rdb_state)).encode(), Constant.TERMINATOR, rdb_state]))
                await writer.drain()
            # Sending the commands to the replicas
            if commands and commands[0].lower() in write_commands:
                commands_bytes = await encode(DataType.ARRAY, [(await encode(DataType.BULK_STRING, command.encode())) for command in commands])
                for _, replica_w in replica_connections:
                    replica_w.write(commands_bytes)
                    await replica_w.drain()
                    print(f'sent {commands_bytes} to replica {
                          replica_w.get_extra_info('peername')}')
        print(f"{replication['role']} - Finished waiting for requests from client - {addr}")
    except Exception as e:
        print(f"{replication['role']} - Error occurred for client {addr}: {e}")
        print(traceback.format_exc())
    finally:
        print(f"{replication['role']} - Closing the connection for client {addr}")
        writer.close()
        await writer.wait_closed()


async def handle_connection_with_master(address,replica_port):
    print(f"{replication['role']} - Starting the master handler coroutine on port {replica_port} with address {address}")
    host, port = address
    reader, writer = await asyncio.open_connection(host, port)
    # Master handshake
    handshake_bytes = [b'*1\r\n$4\r\nping\r\n', await encode(DataType.ARRAY, [
            await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
            await encode(DataType.BULK_STRING, 'listening-port'.encode()),
            await encode(DataType.BULK_STRING, str(replica_port).encode())
        ]),await encode(DataType.ARRAY, [
            await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
            await encode(DataType.BULK_STRING, 'capa'.encode()),
            await encode(DataType.BULK_STRING, 'psync2'.encode())
        ]),await encode(DataType.ARRAY, [
            await encode(DataType.BULK_STRING, Command.PSYNC.encode()),
            await encode(DataType.BULK_STRING, '?'.encode()),
            await encode(DataType.BULK_STRING, '-1'.encode())
        ])]
    handshake_byte = 0
    # sends PING command    
    writer.write(handshake_bytes[handshake_byte])
    await writer.drain()
    # This connection with master, needs to be open for the entire lifetime of the replica
    while not reader.at_eof():
        print(f"{replication['role']} - Keeping master connection alive, waiting on master's response.....")
        response = await reader.read(4096)
        #response = await reader.readUntil(Constant.TERMINATOR) This does not work while reading rdb file
        print(f"{replication['role']} - Received response from master: {response}")
        if response:
            handshake_byte += 1
            if handshake_byte < len(handshake_bytes):
                print(f"{replication['role']} - Sending the next handshake command to master {handshake_byte}")
                writer.write(handshake_bytes[handshake_byte])
                await writer.drain()   
            continue
    print(f"{replication['role']} - Closing the connection with master")
    writer.close()
    await writer.wait_closed()

async def start_server(port):
    server = await asyncio.start_server(connection_handler, "localhost",port)
    print(f"{replication['role']} - Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)
    parser.add_argument('--replicaof', nargs=2, type=str)
    args = parser.parse_args()
    if args.replicaof:
        replication['role'] = 'slave'
    coros = []
    # We need to run the server and the connection with the master concurrently if the role is slave
    server = asyncio.create_task(start_server(args.port or 6379))
    coros.append(server)
    if args.replicaof:
        master_handler = asyncio.create_task(handle_connection_with_master(args.replicaof,args.port))
        coros.append(master_handler)
    await asyncio.gather(*coros)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{replication['role']} Server stopped by the user")
