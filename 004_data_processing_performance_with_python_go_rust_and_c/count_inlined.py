# boilerplate from: https://github.com/nathants/py-csv

import sys

f = bytearray(b'f')[0]
count = 0

### START BOILERPLATE ###
buffer_size = 1024 * 512
starts = [0 for _ in range(1 << 16)] # type: ignore
ends   = [0 for _ in range(1 << 16)] # type: ignore
comma   = bytearray(b',')[0]
newline = bytearray(b'\n')[0]
while True:
    read_buffer = sys.stdin.buffer.read(buffer_size) # read read_buffer size
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
            read_offset = i + 1 # next row starts on the byte following the newline

            ### START CUSTOM CODE ###
            if f == read_buffer[starts[0]]:
                count += 1
            ### END CUSTOM CODE ###

            max = 0 # reset for next row
    if stop:
        break
### END BOILERPLATE ###

print(count)
