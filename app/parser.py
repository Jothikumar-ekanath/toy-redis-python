import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class DataType:
    ARRAY = b'*'
    BULK_STRING = b'$'
    SIMPLE_STRING = b'+'
    SIMPLE_ERROR = b'-'


@dataclass
class Constant:
    NULL_BULK_STRING = b'$-1\r\n'
    TERMINATOR = b'\r\n'
    EMPTY_BYTE = b''
    SPACE_BYTE = b' '
    PONG = b'PONG'
    OK = b'OK'
    INVALID_COMMAND = b'Invalid Command'
    FULLRESYNC = b'FULLRESYNC'


@dataclass
class Command:
    PING = 'ping'
    ECHO = 'echo'
    SET = 'set'
    GET = 'get'
    INFO = 'info'
    REPLCONF = 'replconf'
    PSYNC = 'psync'


class RESPParser:

    async def parse_resp_request(reader: asyncio.StreamReader):
        try:
            _ = await reader.read(1)
            if _ != DataType.ARRAY:
                print(f'Expected {DataType.ARRAY}, got {_}')
                return []

            num_commands = int(await reader.readuntil(Constant.TERMINATOR))
            # note: even though read.readuntil() returns bytes along with the terminator,
            # int() is able to handle bytes and surrounding whitespaces.
            # note: '\r' and '\n' are counted as whitespaces.

            commands = []

            while len(commands) < num_commands:

                datatype = await reader.read(1)

                if datatype == DataType.BULK_STRING:
                    length = int(await reader.readuntil(Constant.TERMINATOR))
                    data = await reader.read(length)

                    _ = await reader.read(2)
                    # terminator not found after `length` bytes
                    if _ != Constant.TERMINATOR:
                        print(f'Expected {Constant.TERMINATOR}, got {_}')
                        return []

                    commands.append(data.decode())

                else:
                    print(f'Expected {DataType.BULK_STRING}, got {datatype}')
                    return []

            return commands

        except Exception as e:
            print(f'An error occurred: {e}')
            return []
        