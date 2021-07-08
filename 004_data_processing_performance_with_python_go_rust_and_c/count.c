#include "csv.h"

int main(int argc, char **argv) {
    // setup io
    CSV_INIT();
    // process input row by row
    i32 count = 0;
    while (1) {
        // parse row
        CSV_READ_LINE(stdin);
        if (csv_stop)
            break;
        ASSERT(csv_max >= 7, "fatal: not enough columns\n");
        // handle row
        ASSERT(csv_sizes[0] > 0, "fatal: no data\n");
        if (csv_columns[0][0] == 'f')
            count += 1;
    }
    printf("%d\n", count);
}
