import socket
import asyncio

async def connection_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    try:
        addr = writer.get_extra_info("peername")
        while True:
            request = await reader.readline()
            if request == b"":
                break
            elif b"ping" in request:
                writer.write(b"+PONG\r\n")
            print(f"sent response to data: {request}, for {addr}")
        await writer.drain()
    except Exception as e:
        print(f"An error occurred for {addr}: {e}")
    finally:
        writer.close()

# Simple connection handler using socket
def handle_connection(client_socket: socket) -> None:
    with client_socket:
        # _ = client_socket.recv(1024)
        # client_socket.sendall("+PONG\r\n".encode())
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            client_socket.sendall("+PONG\r\n".encode())


async def main():
    # print("Logs from your program will appear here!")
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # client_socket, _addr = server_socket.accept() # wait for client
    # handle_connection(client_socket)
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
