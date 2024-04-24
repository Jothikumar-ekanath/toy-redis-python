import asyncio
from app.parser import RESPParser

# to store Key-Value pairs
cache = {}

# coroutine that will start another coroutine after a delay in seconds
async def delay(coro, seconds):
    # suspend for a time limit in seconds
    await asyncio.sleep(seconds)
    # execute the other coroutine
    await coro

async def pop_cache(key: str) -> None:
    print(f"Expiring key {key}")
    cache.pop(key, None)

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
                return b"+PONG\r\n"
            case "ECHO":
                return b"+" + parsed_req[1].encode("utf-8") + b"\r\n"
            case "GET":
                # Example: handling GET command
                key = parsed_req[1]
                value = cache.get(key)
                if value is None:
                    return b"$-1\r\n"
                resp = (
                    b"$" + str(len(value)).encode("utf-8") +
                    b"\r\n" + value.encode() + b"\r\n"
                )
                return resp
            case "SET":
                # Example: handling SET command
                key = parsed_req[1]
                value = parsed_req[2]
                cache[key] = value
                if len(parsed_req) > 3 and parsed_req[3].upper() == "PX":
                    # SET key value PX milliseconds
                    delay_sec = int(parsed_req[4]) / 1000
                    #print(f"Setting key {key} to expire in {delay_sec} seconds")
                    asyncio.create_task(delay(pop_cache(key), delay_sec))
                return b"+OK\r\n"
            case _:
                # Unsupported command
                return b"-ERR\r\n"
    else:
        # Null array or unsupported request
        return b"-ERR\r\n"

# Simple connection handler using asyncio


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


async def main():
    server = await asyncio.start_server(connection_handler, "localhost", 6379)
    print(f"Server running on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
