#include "row.h"
#include "load.h"
#include "write_simple.h"

int main(int argc, char **argv) {
    // setup io
    readbuf_t rbuf = rbuf_init((FILE*[]){stdin}, 1);
    // process input row by row
    i32 count = 0;
    row_t row;
    while (1) {
        // parse row
        load_next(&rbuf, &row, 0);
        if (row.stop)
            break;
        ASSERT(row.max == 7, "fatal: the wrong number of columns\n");
        // handle row
        ASSERT(row.sizes[0] > 0, "fatal: no data\n");
        if (row.columns[0][0] == 'f')
            count += 1;
    }
    printf("%d\n", count);
}
