class RESPParser:
    def __init__(self):
        self.buffer = b""
        self.current_pos = 0

    def feed_data(self, data):
        self.buffer += data
        
    def parse(self):
        while True:
            if self.current_pos >= len(self.buffer):
                break
            if self.buffer[self.current_pos] == 36:  # $
                # Bulk string
                self.current_pos += 1
                string_length = self._parse_integer()
                if self.current_pos + string_length + 2 > len(self.buffer):
                    # Not enough data to parse
                    break
                bulk_string = self.buffer[
                    self.current_pos : self.current_pos + string_length
                ]
                self.current_pos += string_length + 2  # Skip \r\n
                yield bulk_string
            elif self.buffer[self.current_pos] == 42:  # *
                # Array
                self.current_pos += 1
                array_length = self._parse_integer()
                if array_length == -1:
                    # Null array
                    yield None
                    continue
                if self.current_pos + array_length + 2 > len(self.buffer):
                    # Not enough data to parse
                    break
                array_data = []
                for _ in range(array_length):
                    next_data = next(self.parse())
                    if isinstance(next_data, bytes):
                        array_data.append(next_data.decode("utf-8"))
                    else:
                        array_data.append(next_data)
                yield array_data
            else:
                # Simple string, integer, or error
                end_pos = self.buffer.find(b"\r\n", self.current_pos)
                if end_pos == -1:
                    # Not enough data to parse
                    break
                response = self.buffer[self.current_pos : end_pos]
                self.current_pos = end_pos + 2  # Skip \r\n
                yield response

    def _parse_integer(self):
        end_pos = self.buffer.find(b"\r\n", self.current_pos)
        if end_pos == -1:
            # Not enough data to parse
            return None
        integer_str = self.buffer[self.current_pos : end_pos]
        self.current_pos = end_pos + 2  # Skip \r\n
        return int(integer_str)