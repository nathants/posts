#pragma once

#include "util.h"

typedef struct row_s {
    i32 stop;
    i32 max;
    i32 sizes[MAX_COLUMNS];
    u8 *columns[MAX_COLUMNS];
} row_t;
