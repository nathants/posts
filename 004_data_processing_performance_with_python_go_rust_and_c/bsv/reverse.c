#include "row.h"
#include "load.h"
#include "write_simple.h"

int main(int argc, char **argv) {
    // setup io
    readbuf_t rbuf = rbuf_init((FILE*[]){stdin}, 1);
    writebuf_t wbuf = wbuf_init((FILE*[]){stdout}, 1);
    // process input row by row
    row_t row;
    while (1) {
        // parse row
        load_next(&rbuf, &row, 0);
        if (row.stop)
            break;
        ASSERT(row.max == 7, "fatal: the wrong number of columns\n");
        // handle row
        write_bytes(&wbuf, row.columns[7], row.sizes[7], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[6], row.sizes[6], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[5], row.sizes[5], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[4], row.sizes[4], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[3], row.sizes[3], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[2], row.sizes[2], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[1], row.sizes[1], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, row.columns[0], row.sizes[0], 0); write_bytes(&wbuf, "\n", 1, 0);
    }
    write_flush(&wbuf, 0);
}
