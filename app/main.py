import asyncio
from app.parser import RESPParser, DataType, Constant, Command
import argparse
import traceback

# Global server state, irrespective of the number of clients or replicas connecting to server
# to store Key-Value pairs
# Locks to ensure thread safety
cache_lock = asyncio.Lock()
replica_connections_lock = asyncio.Lock()

cache = {} # Mutable
replica_connections = [] # mutable
replica_ack_queue = asyncio.Queue()

# Mutable variables, but written only at the start of the server, after that its all READ-ONLY
write_commands = set([Command.SET])
rdb_state = bytes.fromhex(
    '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')
replication = {
    'role': 'master',
    'connected_slaves': 0,
    'master_replid': '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
    'master_repl_offset': 0
}
dir = None
dbfilename = None

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
    if datatype == DataType.INTEGER:
        return Constant.TERMINATOR.join([b':' + data, Constant.EMPTY_BYTE])

async def execute_resp_commands(commands: list[str] | None,writer: asyncio.StreamWriter) -> None:
    response = None
    if not commands:
        response = await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)
    else:  # Treat all other RESP commands
        cmd = commands[0].lower()
        match cmd:
            case Command.PING:
                response =  await encode(DataType.SIMPLE_STRING, Constant.PONG)
            case Command.ECHO:
                response =  await encode(DataType.SIMPLE_STRING, commands[1].encode())
            case Command.GET:
                # Example: handling GET command
                key = commands[1]
                value = cache.get(key, None)
                if value is None:
                    response =  Constant.NULL_BULK_STRING
                else:
                    response =  await encode(DataType.BULK_STRING, value.encode())
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
                response =  await encode(DataType.SIMPLE_STRING, Constant.OK)
            case Command.INFO:
                data = '\n'.join(
                    [f'{key}:{value}' for key, value in replication.items()]).encode()
                response =  await encode(DataType.BULK_STRING, data)
            case Command.REPLCONF:
                # Handshake commands from replica's
                if replication['role'] == 'master': 
                    if commands[1].lower() == 'listening-port':
                        async with replica_connections_lock:
                            replica_connections.append(writer)
                        print(f"{replication['role']} - Replica connections added")
                        response =  await encode(DataType.SIMPLE_STRING, Constant.OK)
                    elif commands[1].lower() == 'capa':
                        response =  await encode(DataType.SIMPLE_STRING, Constant.OK)
                    elif commands[1].lower() == Command.ACK:
                        await replica_ack_queue.put("1")
                        print(f"{replication['role']} - ACK received from replica, Total ACKs: {replica_ack_queue.qsize()}")  
            case Command.PSYNC:
                # Handshake commands from replica's
                if replication['role'] == 'master':
                    await execute_resp_commands([Command.FULLRESYNC],writer)
                    # sending rdb file
                    response = Constant.EMPTY_BYTE.join([DataType.BULK_STRING, str(len(rdb_state)).encode(), Constant.TERMINATOR, rdb_state])
            case Command.FULLRESYNC:
                if replication['role'] == 'master':
                   response =  await encode(
                    DataType.SIMPLE_STRING,
                    Constant.SPACE_BYTE.join([
                        Constant.FULLRESYNC,
                        replication['master_replid'].encode(),
                        str(replication['master_repl_offset']).encode()
                    ]))
            case Command.WAIT:
                response =  None # Handled in handle_wait coro
            case Command.CONFIG:
                if commands[1].lower() == 'get' and commands[2].lower() == 'dir':
                    response = await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, "dir".encode()),await encode(DataType.BULK_STRING, dir.encode())])
                elif commands[1].lower() == 'get' and commands[2].lower() == 'dbfilename':
                    response = await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, "dbfilename".encode()),await encode(DataType.BULK_STRING, dbfilename.encode())])
            case _: # unrecognized command, not handled, return error
                response = await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)
    if writer and response is not None:
        print(f"{replication['role']} - writing response: {response} to client {writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}")
        writer.write(response)
        await writer.drain()

async def handle_wait(commands: list[str],writer: asyncio.StreamWriter,previous_command: str) -> None:
    if commands[0].lower() == Command.WAIT:
        addr = writer.get_extra_info('peername')
        wait_time_in_sec = float(commands[2])/1000
        expected_replica_ack_count = int(commands[1])
        async with replica_connections_lock:
            replicas_count = len(replica_connections)
        if previous_command == Command.SET and expected_replica_ack_count > 0:
            # send GET ACK command to all replicas to acknowledge the command
            async with replica_connections_lock:
                for replica_w in replica_connections:
                    replica_w.write(await encode(DataType.ARRAY, [(await encode(DataType.BULK_STRING, command.encode())) for command in ["REPLCONF", "GETACK", '*']]))
                    await replica_w.drain()
                    print(f'sent REPLCONF GETACK * to replica {replica_w.get_extra_info('peername')}')
                await asyncio.sleep(wait_time_in_sec)
                replicas_count = replica_ack_queue.qsize() 
        writer.write(await encode(DataType.INTEGER, str(replicas_count).encode()))
        await writer.drain()
        print(f"{replication['role']} - Sent replica count: {replicas_count} to client {addr[0]}:{addr[1]}")
         # Clear the replica_ack_queue
        while not replica_ack_queue.empty():
            await replica_ack_queue.get()
            replica_ack_queue.task_done()

async def replicate_commands_to_replicas(commands: list[str]) -> None:
    if commands[0].lower() in write_commands:
        commands_bytes = await encode(DataType.ARRAY, [(await encode(DataType.BULK_STRING, command.encode())) for command in commands])
        async with replica_connections_lock:
            for replica_w in replica_connections:
                replica_w.write(commands_bytes)
                await replica_w.drain()
                print(f'sent {commands_bytes} to replica {replica_w.get_extra_info('peername')}')

# Each client connection is handled by this function, All local variable belongs to the invividual client session
async def connection_handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        addr = writer.get_extra_info('peername')
        previous_command = None
        while reader.at_eof() is False:
            original,commands = await RESPParser.parse_resp_array_request(reader)
            print(f"{replication['role']} - command: {original}, length: {len(original)}, parsed: {commands} from client {addr[0]}:{addr[1]}")
            if commands is None:
                break
            await execute_resp_commands(commands,writer) 
            # Sending the commands to the replicas if needed
            await replicate_commands_to_replicas(commands)
            await handle_wait(commands,writer,previous_command)
            previous_command = commands[0].lower()
        print(f"{replication['role']} - Connection closed from client {addr[0]}:{addr[1]}")
    except asyncio.IncompleteReadError as re:
        print(f"{replication['role']} - IncompleteReadError from client {addr[0]}:{addr[1]}, {re}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"{replication['role']} - Error occurred for client {addr[0]}:{addr[1]}: {e}")
        print(traceback.format_exc())
    finally:
        print(f"{replication['role']} - Closing the connection for client {addr[0]}:{addr[1]}")
        writer.close()
        await writer.wait_closed()

async def handle_connection_with_master(address, replica_port):
    print(f"{replication['role']} - Handshake start with master {address} from replica port {replica_port}")
    host, port = address
    reader, writer = await asyncio.open_connection(host, port)
    # Master handshake
    handshake_bytes = [b'*1\r\n$4\r\nping\r\n', await encode(DataType.ARRAY, [
        await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
        await encode(DataType.BULK_STRING, 'listening-port'.encode()),
        await encode(DataType.BULK_STRING, str(replica_port).encode())
    ]), await encode(DataType.ARRAY, [
        await encode(DataType.BULK_STRING, Command.REPLCONF.encode()),
        await encode(DataType.BULK_STRING, 'capa'.encode()),
        await encode(DataType.BULK_STRING, 'psync2'.encode())
    ]), await encode(DataType.ARRAY, [
        await encode(DataType.BULK_STRING, Command.PSYNC.encode()),
        await encode(DataType.BULK_STRING, '?'.encode()),
        await encode(DataType.BULK_STRING, '-1'.encode())
    ])]

    # sends PING command
    writer.write(handshake_bytes[0])
    await writer.drain()
    await reader.readuntil(Constant.TERMINATOR)
    # sends REPLCONF listening-port <port>
    writer.write(handshake_bytes[1])
    await writer.drain()
    await reader.readuntil(Constant.TERMINATOR)
    # sends REPLCONF capa psync2
    writer.write(handshake_bytes[2])
    await writer.drain()
    await reader.readuntil(Constant.TERMINATOR)
    # sends PSYNC ? -1
    writer.write(handshake_bytes[3])
    await writer.drain()
    # receive FULLRESYNC response
    await reader.readuntil(Constant.TERMINATOR)
    # read empty RDB file
    res = await reader.readuntil(Constant.TERMINATOR)
    await reader.readexactly(int(res[1:-2]))
    print(f"{replication['role']} - Handshake completed with master {address} from replica port {replica_port}")

    # Process all master commands
    command_count = 0
    while not reader.at_eof():
        original,response = await RESPParser.parse_resp_array_request(reader)
        print(f"{replication['role']} - Received command from master: {response}, length: {len(original)}, original: {original}")
        if response and response[0].lower() in {Command.SET, Command.PING}:
            command_count += len(original)
            await execute_resp_commands(response,None)

            # # Introducing new Command.ACK to acknowledge the set command
            # if response[0].lower() == Command.SET:
            #     writer.write(await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, Command.ACK.encode()),await encode(DataType.BULK_STRING, str(command_count).encode())]))
            #     await writer.drain()

        if response and response[0].lower() == Command.REPLCONF and response[1].lower() == Command.GETACK and response[2] == '*':
            print(f"{replication['role']} - Received GETACK from master")
            # '*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$3\r\n154\r\n'
            writer.write(await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, Command.REPLCONF.encode()), await encode(DataType.BULK_STRING, 'ACK'.encode()),
                                                       await encode(DataType.BULK_STRING, str(command_count).encode())]))
            await writer.drain()
            command_count += len(original)

    print(f"{replication['role']} - Closing the connection with master")
    writer.close()
    await writer.wait_closed()

async def start_server(port):
    server = await asyncio.start_server(connection_handler, "localhost", port)
    print(
        f"{replication['role']} - Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)
    parser.add_argument('--replicaof', nargs=2, type=str)
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)
    args = parser.parse_args()
    if args.replicaof:
        replication['role'] = 'slave'
    global dir, dbfilename
    if args.dir:
        dir = args.dir
    if args.dbfilename:
        dbfilename = args.dbfilename
    coros = []
    # We need to run the server and the connection with the master concurrently if the role is slave
    server = asyncio.create_task(start_server(args.port or 6379))
    coros.append(server)
    if args.replicaof:
        master_handler = asyncio.create_task(
            handle_connection_with_master(args.replicaof, args.port))
        coros.append(master_handler)
    await asyncio.gather(*coros)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{replication['role']} Server stopped by the user")
