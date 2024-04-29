import asyncio
from app.parser import RESPParser
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


class ResponseType:
    ARRAY = b'*'
    BULK_STRING = b'$'
    SIMPLE_STRING = b'+'
    SIMPLE_ERROR = b"-ERR\r\n"
    OK = b'+OK\r\n'
    PONG = b'+PONG\r\n'

# response enum
# 1. Simple String: +OK\r\n
# 2. Error: -ERR\r\n
# 3. Integer: :1000\r\n
# 4. Bulk String: $6\r\nfoobar\r\n
# 5. Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
# 6. Null Bulk String: $-1\r\n
# 7. Null Array: *-1\r\n


async def generate_response(value: str, type: ResponseType) -> bytes:
    if type == ResponseType.BULK_STRING:
        if value is None:
            return b"$-1\r\n"
        resp = (
            b"$" + str(len(value)).encode("utf-8") +
            b"\r\n" + value.encode() + b"\r\n"
        )
        return resp
    if type == ResponseType.SIMPLE_STRING:
        return b"+" + value.encode("utf-8") + b"\r\n"


async def handle_request(parsed_req: bytes | list[bytes] | None) -> bytes:
    # Placeholder implementation, replace with actual request handling logic
    if isinstance(parsed_req, bytes):
        # Simple string or integer
        return b"+" + parsed_req + b"\r\n"
    elif isinstance(parsed_req, list):
        # Array
        cmd = parsed_req[0].upper()
        match cmd:
            case "PING":
                return ResponseType.PONG
            case "ECHO":
                return await generate_response(parsed_req[1], ResponseType.SIMPLE_STRING)
            case "GET":
                # Example: handling GET command
                key = parsed_req[1]
                value = cache.get(key)
                return await generate_response(value, ResponseType.BULK_STRING)
            case "SET":
                # Example: handling SET command
                key = parsed_req[1]
                value = parsed_req[2]
                cache[key] = value
                if len(parsed_req) > 3 and parsed_req[3].upper() == "PX":
                    # SET key value PX milliseconds
                    delay_sec = int(parsed_req[4]) / 1000
                    # print(f"Setting key {key} to expire in {delay_sec} seconds")
                    asyncio.create_task(delay(pop_cache(key), delay_sec))
                return ResponseType.OK
            case "INFO":
                data = '\n'.join(
                    [f'{key}:{value}' for key, value in replication.items()])
                return await generate_response(data, ResponseType.BULK_STRING)
            case _:
                # Unsupported command
                return ResponseType.SIMPLE_ERROR
    else:
        # Null array or unsupported request
        return ResponseType.SIMPLE_ERROR


async def connection_handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        addr = writer.get_extra_info("peername")
        parser = RESPParser()
        while reader.at_eof() is False:
            command = await reader.read(1024)
            print(f"received command: {command}")
            if not command:
                # print(f"Connection closed by {addr} for {command}")
                break
            else:
                parser.feed_data(command)
                for parsed_req in parser.parse():
                    print(f"RESP parsed_req: {parsed_req}")
                    response = await handle_request(parsed_req)
                    writer.write(response)
                    await writer.drain()
    except Exception as e:
        print(f"An error occurred for {addr}: {e}")
    finally:
        writer.close()
        # await writer.wait_closed()


async def send_handshake(address):
    host, port = address
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(b'*1\r\n$4\r\nping\r\n')  # send PING command
        await writer.drain()
    finally:
        writer.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)
    parser.add_argument('--replicaof', nargs=2, type=str)
    args = parser.parse_args()
    if args.replicaof:
        replication['role'] = 'slave'
        await send_handshake(args.replicaof)

    server = await asyncio.start_server(connection_handler, "localhost", args.port or 6379)
    print(f"Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
