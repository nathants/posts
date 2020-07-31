import sys
import collections

sys.stdin.readline() # skip the header

result = collections.defaultdict(int)

### START BOILERPLATE ###
buffer_size = 1024 * 512
starts = [0 for _ in range(1 << 16)]
ends   = [0 for _ in range(1 << 16)]
comma = bytearray(b',')[0]
newline = bytearray(b'\n')[0]
while True:
    read_buffer = sys.stdin.buffer.read(buffer_size) # type: ignore
    stop = len(read_buffer) != buffer_size
    if len(read_buffer) == buffer_size: # on a full read, extend with the next full line so the read_buffer always ends with a newline
        read_buffer += sys.stdin.buffer.readline()
    read_offset = 0
    max = 0
    for i in range(len(read_buffer)): # process read_buffer byte by byte
        if read_buffer[i] == comma: # found the next column
            starts[max] = read_offset
            ends[max] = i
            read_offset = i + 1
            max += 1
        elif read_buffer[i] == newline: # found the row end
            starts[max] = read_offset
            ends[max] = i
            read_offset = i

            ### START CUSTOM CODE ###
            if max >= 3:
                passengers = read_buffer[starts[3]:ends[3]]
                result[passengers] += 1
            ### END CUSTOM CODE ###

            max = 0 # reset for next row
    if stop:
        break
### END BOILERPLATE ###

# print passenger counts to stdout
for passengers, count in result.items():
    print(f'{passengers.decode()},{count}')
