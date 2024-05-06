import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class DataType:
    ARRAY = b'*'
    BULK_STRING = b'$'
    SIMPLE_STRING = b'+'
    SIMPLE_ERROR = b'-'
    INTEGER = b':'


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
    ACK = 'ack'
    GETACK = 'getack'
    PSYNC = 'psync'
    FULLRESYNC = 'fullresync'
    WAIT= 'wait'
    CONFIG = 'config'
   


class RESPParser:

    async def parse_resp_array_request(reader: asyncio.StreamReader):
        original = b''
        try:
            first_byte = await reader.read(1)
            original += first_byte
            if first_byte == Constant.EMPTY_BYTE:
                return original,None
            if first_byte != DataType.ARRAY:
                print(f'Expected {DataType.ARRAY}, got {first_byte}')
                return original,[]

            num_commands = await reader.readuntil(Constant.TERMINATOR)
            original += num_commands
            num_commands_int = int(num_commands)
            
            # note: even though read.readuntil() returns bytes along with the terminator,
            # int() is able to handle bytes and surrounding whitespaces.
            # note: '\r' and '\n' are counted as whitespaces.

            parsed = []

            while len(parsed) < num_commands_int:

                datatype = await reader.read(1)
                original += datatype
                if datatype == DataType.BULK_STRING:
                    length = await reader.readuntil(Constant.TERMINATOR)
                    original += length
                    length = int(length)
                    data = await reader.read(length)
                    original += data
                    first_byte = await reader.read(2)
                    original += first_byte
                    # terminator not found after `length` bytes
                    if first_byte != Constant.TERMINATOR:
                        print(f'Expected {Constant.TERMINATOR}, got {first_byte}')
                        return original,[]

                    parsed.append(data.decode())

                else:
                    print(f'Expected {DataType.BULK_STRING}, got {datatype}')
                    return original,[]
            return original,parsed

        except Exception as e:
            print(f'An error occurred: {e}')
            return original,[]
        