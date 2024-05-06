import struct
from datetime import datetime
from enum import Enum
from typing import Optional

class RDBConfig:
    def __init__(self, dir: Optional[str] = None, dbfilename: Optional[str] = None):
        self.dir = dir
        self.dbfilename = dbfilename

class RDB_OP(Enum):
    EOF = b"\xff"
    SELECTDB = b"\xfe"
    EXPIRETIME = b"\xfd"
    EXPIRETIMEMS = b"\xfc"
    RESIZEDB = b"\xfb"
    AUX = b"\xfa"

class ValueType(Enum):
    STRING = 0

def decode_length(bs: bytes) -> tuple[int, int]:
    first = bs[0]
    sig_bits = (first >> 6) & 3  # SHIFT to right and AND with 11
    length = 0
    end = 0
    match sig_bits:
        case 0:
            length = first & 0b00111111
            end = 1
        case 1:
            fb = first & 0b00111111
            length = int.from_bytes(bytes([fb, bs[1]]), "little")
            end = 2
        case 2:
            length = int.from_bytes(bs[1:5], "little")
            end = 5
        case 3:
            pass
    return (length, end)

def decode_string(bs: bytes) -> tuple[str, int]:
    length, end = decode_length(bs)
    string = bs[end : end + length].decode()
    end = end + length
    return (string, end)

def read_rdb_file(rdb_config: RDBConfig) -> Optional[bytes]:
    try:
        f = open(f"{rdb_config.dir}/{rdb_config.dbfilename}", "rb")
        return f.read()
    except OSError:
        return None
    
def parse_rdb_db(db: bytes) -> list[tuple[str, str]]:
    res = []
    i = 0
    while i < len(db):
        expiry = None
        if db[i : i + 1] == RDB_OP.EXPIRETIME.value:
            i += 1
            # unsigned int (4 bytes)
            expiry_time_sec = int(struct.unpack("<I", db[i : i + 4])[0])
            expiry = datetime.fromtimestamp(expiry_time_sec)
            i += 4
        elif db[i : i + 1] == RDB_OP.EXPIRETIMEMS.value:
            i += 1
            # unsigned long (8 bytes)
            expiry_time_ms = int(struct.unpack("<Q", db[i : i + 8])[0])
            expiry = datetime.fromtimestamp(expiry_time_ms / 1000)
            i += 8
        val_type = db[i]
        if val_type == ValueType.STRING.value:
            i += 1
            key, end = decode_string(db[i:])
            i += end
            value, end = decode_string(db[i:])
            i += end
            res.append((key, (value, expiry)))
    return res

def parse_rdb_file(rdb_config: RDBConfig) -> list[tuple[str, str]]:
    read_bytes = read_rdb_file(rdb_config)
    if not read_bytes:
        return []
    res = []
    i = 10  # Skip over REDIS, version number and AUX opcode
    while read_bytes[i : i + 1] != RDB_OP.EOF.value:
        while read_bytes[i : i + 1] != RDB_OP.RESIZEDB.value:
            i += 1
        i += 1  # skip over resizedb opcode
        hash_table_size, end = decode_length(read_bytes[i:])
        i += end
        expire_hash_table_size, end = decode_length(read_bytes[i:])
        i += end
        end = i
        while read_bytes[end : end + 1] not in [
            RDB_OP.SELECTDB.value,
            RDB_OP.EOF.value,
        ]:
            end += 1
        res += parse_rdb_db(read_bytes[i:end])
        i = end
    return res

def get_rdb_map(rdb_config: RDBConfig) -> dict:
    kvs = parse_rdb_file(rdb_config)
    return dict(kvs)