#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define inlined inline __attribute__((always_inline))

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef float    f32;
typedef double   f64;

i32 _i32;
u16 _u16;
u8 _u8;

#define TO_UINT16(src) (_u16 = (u16)(src), (u8*)&_u16)

#define FROM_UINT16(src) (*(u16*)(src))

#define BUFFER_SIZE 1024 * 1024 * 5

#define MAX_COLUMNS 65535

#define MALLOC(dst, size)                                           \
    do {                                                            \
        dst = malloc(size);                                         \
        ASSERT(dst != NULL, "fatal: failed to allocate memory\n");  \
    } while(0)

#define DELIMITER ','

#define ASSERT(cond, ...)                       \
    do {                                        \
        if (!(cond)) {                          \
            fprintf(stderr, ##__VA_ARGS__);     \
            exit(1);                            \
        }                                       \
    } while(0)

#define FREAD(buffer, size, file)                                                                   \
    do {                                                                                            \
        _i32 = fread_unlocked(buffer, 1, size, file);                                               \
        ASSERT(size == _i32, "fatal: failed to read input, expected %d got %d\n", (i32)size, _i32); \
    } while(0)

#define FWRITE(buffer, size, file)                                                                      \
    do {                                                                                                \
        _i32 = fwrite_unlocked(buffer, 1, size, file);                                                  \
        ASSERT(size == _i32, "fatal: failed to write output, expected %d got %d\n", (i32)size, _i32);   \
        ASSERT(0 == fflush_unlocked(file), "fatal: failed to flush\n");                                 \
    } while(0)
