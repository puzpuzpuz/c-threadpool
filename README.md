# c-threadpool

Experiments with thread pool implementations in C.

## Building and running

Supposed to be run on Linux. Tested on Ubuntu 20.04 with gcc 9.3.0.

```bash
$ gcc -pthread -O3 -o single-q-pool single-q-pool.c
$ ./single-q-pool
$ gcc -pthread -O3 -o multi-q-pool multi-q-pool.c
$ ./multi-q-pool
```

## Copyright notices

Some parts of the code are based on [libuv](https://github.com/libuv/libuv).
Thus, all corresponding copyrights apply.
