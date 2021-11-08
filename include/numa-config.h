// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

enum {
    NUM_SOCKET = 2,
    NUM_PHYSICAL_CPU_PER_SOCKET = 16,
    SMT_LEVEL = 2,
};

const int OS_CPU_ID[NUM_SOCKET][NUM_PHYSICAL_CPU_PER_SOCKET][SMT_LEVEL] = {
    { /* socket id: 0 */
        { /* physical cpu id: 0 */
          0, 32,     },
        { /* physical cpu id: 1 */
          1, 33,     },
        { /* physical cpu id: 2 */
          2, 34,     },
        { /* physical cpu id: 3 */
          3, 35,     },
        { /* physical cpu id: 4 */
          4, 36,     },
        { /* physical cpu id: 5 */
          5, 37,     },
        { /* physical cpu id: 6 */
          6, 38,     },
        { /* physical cpu id: 7 */
          7, 39,     },
        { /* physical cpu id: 8 */
          8, 40,     },
        { /* physical cpu id: 9 */
          9, 41,     },
        { /* physical cpu id: 10 */
          10, 42,     },
        { /* physical cpu id: 11 */
          11, 43,     },
        { /* physical cpu id: 12 */
          12, 44,     },
        { /* physical cpu id: 13 */
          13, 45,     },
        { /* physical cpu id: 14 */
          14, 46,     },
        { /* physical cpu id: 15 */
          15, 47,     },
    },
    { /* socket id: 1 */
        { /* physical cpu id: 0 */
          16, 48,     },
        { /* physical cpu id: 1 */
          17, 49,     },
        { /* physical cpu id: 2 */
          18, 50,     },
        { /* physical cpu id: 3 */
          19, 51,     },
        { /* physical cpu id: 4 */
          20, 52,     },
        { /* physical cpu id: 5 */
          21, 53,     },
        { /* physical cpu id: 6 */
          22, 54,     },
        { /* physical cpu id: 7 */
          23, 55,     },
        { /* physical cpu id: 8 */
          24, 56,     },
        { /* physical cpu id: 9 */
          25, 57,     },
        { /* physical cpu id: 10 */
          26, 58,     },
        { /* physical cpu id: 11 */
          27, 59,     },
        { /* physical cpu id: 12 */
          28, 60,     },
        { /* physical cpu id: 13 */
          29, 61,     },
        { /* physical cpu id: 14 */
          30, 62,     },
        { /* physical cpu id: 15 */
          31, 63,     },
    },
};
