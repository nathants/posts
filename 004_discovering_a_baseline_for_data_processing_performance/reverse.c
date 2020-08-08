#include "csv.h"
#include "write_simple.h"

int main(int argc, char **argv) {
    // setup io
    CSV_INIT();
    writebuf_t wbuf = wbuf_init((FILE*[]){stdout}, 1);
    // process input row by row
    while (1) {
        // parse row
        CSV_READ_LINE(stdin);
        if (csv_stop)
            break;
        ASSERT(csv_max >= 7, "fatal: not enough columns\n");
        // handle row
        write_bytes(&wbuf, csv_columns[7], csv_sizes[7], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[6], csv_sizes[6], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[5], csv_sizes[5], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[4], csv_sizes[4], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[3], csv_sizes[3], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[2], csv_sizes[2], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[1], csv_sizes[1], 0); write_bytes(&wbuf, ",", 1, 0);
        write_bytes(&wbuf, csv_columns[0], csv_sizes[0], 0); write_bytes(&wbuf, "\n", 1, 0);
    }
    write_flush(&wbuf, 0);
}
