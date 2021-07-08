#include "row.h"
#include "csv.h"
#include "dump.h"

int main(int argc, char **argv) {
    // setup io
    CSV_INIT();
    writebuf_t wbuf = wbuf_init((FILE*[]){stdout}, 1);
    // process input row by row
    row_t row;
    while (1) {
        // parse row
        CSV_READ_LINE(stdin);
        if (csv_stop)
            break;
        ASSERT(csv_max == 7, "fatal: the wrong number of columns\n");
        // handle row
        for (i32 i = 0; i < 8; i++) {
            row.columns[i] = csv_columns[i];
            row.sizes[i] = csv_sizes[i];
        }
        row.max = 7;
        dump(&wbuf, &row, 0);
    }
    dump_flush(&wbuf, 0);
}
