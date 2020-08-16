# boilerplate from: https://github.com/nathants/py-csv

import sys

### START BOILERPLATE ###
buffer_size = 1024 * 512
starts = [0 for _ in range(1 << 16)] # type: ignore
ends   = [0 for _ in range(1 << 16)] # type: ignore
comma   = bytearray(b',')[0]
newline = bytearray(b'\n')[0]
write_buffer = bytearray(buffer_size)
while True:
    read_buffer = sys.stdin.buffer.read(buffer_size) # read read_buffer size
    stop = len(read_buffer) != buffer_size
    if len(read_buffer) == buffer_size: # on a full read, extend with the next full line so the read_buffer always ends with a newline
        read_buffer += sys.stdin.buffer.readline()
    read_offset = 0
    write_offset = 0
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
            val = b''
            val += read_buffer[starts[7]:ends[7]] + b','
            val += read_buffer[starts[6]:ends[6]] + b','
            val += read_buffer[starts[5]:ends[5]] + b','
            val += read_buffer[starts[4]:ends[4]] + b','
            val += read_buffer[starts[3]:ends[3]] + b','
            val += read_buffer[starts[2]:ends[2]] + b','
            val += read_buffer[starts[1]:ends[1]] + b','
            val += read_buffer[starts[0]:ends[0]] + b'\n'
            ### END CUSTOM CODE ###

            if len(val) > len(write_buffer) - write_offset: # maybe flush and write
                sys.stdout.buffer.write(write_buffer[:write_offset])
                write_offset = 0
            write_buffer[write_offset:write_offset + len(val)] = val
            write_offset += len(val)
            max = 0 # reset for next row
    sys.stdout.buffer.write(write_buffer[:write_offset]) # flush
    if stop:
        break
### END BOILERPLATE ###
