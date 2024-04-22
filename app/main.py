import socket


def handle_connection(client_socket: socket) -> None:
    with client_socket:
        # _ = client_socket.recv(1024)
        # client_socket.sendall("+PONG\r\n".encode())
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            client_socket.sendall("+PONG\r\n".encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket, _addr = server_socket.accept() # wait for client
    handle_connection(client_socket)


if __name__ == "__main__":
    main()
