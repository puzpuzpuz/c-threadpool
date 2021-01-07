/* Some parts of the code are based on libuv (https://github.com/libuv/libuv).
 * Thus, all corresponding authorships apply.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>

/* Q start */

typedef void *QUEUE[2];

/* Private macros. */
#define QUEUE_NEXT(q)       (*(QUEUE **) &((*(q))[0]))
#define QUEUE_PREV(q)       (*(QUEUE **) &((*(q))[1]))
#define QUEUE_PREV_NEXT(q)  (QUEUE_NEXT(QUEUE_PREV(q)))
#define QUEUE_NEXT_PREV(q)  (QUEUE_PREV(QUEUE_NEXT(q)))

/* Public macros. */
#define QUEUE_DATA(ptr, type, field)                                          \
  ((type *) ((char *) (ptr) - offsetof(type, field)))

#define QUEUE_EMPTY(q)                                                        \
  ((const QUEUE *) (q) == (const QUEUE *) QUEUE_NEXT(q))

#define QUEUE_HEAD(q)                                                         \
  (QUEUE_NEXT(q))

#define QUEUE_INIT(q)                                                         \
  do {                                                                        \
    QUEUE_NEXT(q) = (q);                                                      \
    QUEUE_PREV(q) = (q);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_INSERT_TAIL(h, q)                                               \
  do {                                                                        \
    QUEUE_NEXT(q) = (h);                                                      \
    QUEUE_PREV(q) = QUEUE_PREV(h);                                            \
    QUEUE_PREV_NEXT(q) = (q);                                                 \
    QUEUE_PREV(h) = (q);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_REMOVE(q)                                                       \
  do {                                                                        \
    QUEUE_PREV_NEXT(q) = QUEUE_NEXT(q);                                       \
    QUEUE_NEXT_PREV(q) = QUEUE_PREV(q);                                       \
  }                                                                           \
  while (0)

/* Q end */

/* threadpool start */

#define THREADPOOL_SIZE 4
#define THREADPOOL_POST_SPINS 2

struct work_s {
  void (*work)(struct work_s *w);
  void* wq[2];
  unsigned int res;
};

struct w_thread_s {
  pthread_t thread;
  char idle;
  pthread_cond_t cond;
  pthread_mutex_t mutex;
  QUEUE queue;
};

struct w_thread_args_s {
  sem_t* sem;
  unsigned int n;
};

static unsigned int nthreads = THREADPOOL_SIZE;
static QUEUE exit_message;
static unsigned int post_n = 0;
static struct w_thread_s w_threads[THREADPOOL_SIZE];


/* On Linux, threads created by musl have a much smaller stack than threads
 * created by glibc (80 vs. 2048 or 4096 kB.)  Follow glibc for consistency.
 */
static size_t pool__thread_stack_size(void) {
  return 2 << 20;
}

int pool__thread_create(pthread_t *tid,
                        void (*entry)(void *arg),
                        void *arg) {
  int err;
  pthread_attr_t* attr;
  pthread_attr_t attr_storage;
  size_t pagesize;
  size_t stack_size;

  /* Used to squelch a -Wcast-function-type warning. */
  union {
    void (*in)(void*);
    void* (*out)(void*);
  } f;

  stack_size = pool__thread_stack_size();

  attr = NULL;
  if (stack_size > 0) {
    attr = &attr_storage;

    if (pthread_attr_init(attr))
      abort();

    if (pthread_attr_setstacksize(attr, stack_size))
      abort();
  }

  f.in = entry;
  err = pthread_create(tid, attr, f.out, arg);

  if (attr != NULL)
    pthread_attr_destroy(attr);

  return err;
}

int pool__thread_join(pthread_t *tid) {
  if (pthread_join(*tid, NULL))
      abort();
}

int pool__mutex_init(pthread_mutex_t* mutex) {
  return pthread_mutex_init(mutex, NULL);
}

void pool__mutex_lock(pthread_mutex_t* mutex) {
  if (pthread_mutex_lock(mutex))
    abort();
}

int pool__mutex_trylock(pthread_mutex_t* mutex) {
  int err;

  err = pthread_mutex_trylock(mutex);
  if (err) {
    if (err != EBUSY && err != EAGAIN)
      abort();
    return err;
  }

  return 0;
}

void pool__mutex_unlock(pthread_mutex_t* mutex) {
  if (pthread_mutex_unlock(mutex))
    abort();
}

void pool__sem_post(sem_t* sem) {
  if (sem_post(sem))
    abort();
}

static void pool__sem_wait(sem_t* sem) {
  int r;

  do
    r = sem_wait(sem);
  while (r == -1 && errno == EINTR);

  if (r)
    abort();
}

static void pool__sem_destroy(sem_t* sem) {
  if (sem_destroy(sem))
    abort();
}

int pool__cond_init(pthread_cond_t* cond) {
  pthread_condattr_t attr;
  int err;

  err = pthread_condattr_init(&attr);
  if (err)
    return err;

  err = pthread_cond_init(cond, &attr);
  if (err)
    goto error2;

  err = pthread_condattr_destroy(&attr);
  if (err)
    goto error;

  return 0;

error:
  pthread_cond_destroy(cond);
error2:
  pthread_condattr_destroy(&attr);
  return err;
}

void pool__cond_signal(pthread_cond_t* cond) {
  if (pthread_cond_signal(cond))
    abort();
}

void pool__cond_wait(pthread_cond_t* cond,
                     pthread_mutex_t* mutex) {
  if (pthread_cond_wait(cond, mutex))
    abort();
}


static void pool__worker(void* arg) {
  unsigned int n, i;
  struct w_thread_s* wt;
  struct work_s* w;
  QUEUE* q;

  n = ((struct w_thread_args_s*) arg)->n;
  pool__sem_post(((struct w_thread_args_s*) arg)->sem);
  arg = NULL;

  for (;;) {
    // work stealing
    for (i = 0; i < nthreads; i++) {
      wt = w_threads + ((i + n) % nthreads);
      if (pool__mutex_trylock(&wt->mutex) == 0) {
        if (QUEUE_EMPTY(&wt->queue)) {
          pool__mutex_unlock(&wt->mutex);
          wt = NULL;
          continue;
        }
        break;
      } else {
        wt = NULL;
      }
    }

    // could not steal, so fallback to pessimistic mode
    if (wt == NULL) {
      wt = w_threads + n;
      pool__mutex_lock(&wt->mutex);
      while (QUEUE_EMPTY(&wt->queue)) {
        wt->idle = 1;
        pool__cond_wait(&wt->cond, &wt->mutex);
        wt->idle = 0;
      }
    }

    q = QUEUE_HEAD(&wt->queue);

    if (q == &exit_message)
      pool__cond_signal(&wt->cond);
    else {
      QUEUE_REMOVE(q);
      QUEUE_INIT(q);
    }

    pool__mutex_unlock(&wt->mutex);

    if (q == &exit_message)
      break;

    w = QUEUE_DATA(q, struct work_s, wq);
    w->work(w);
  }
}


static void pool__post(QUEUE* q) {
  unsigned int n, i;
  struct w_thread_s* wt;

  n = post_n++;
  // optimistic post mode
  for (i = 0; i < nthreads * THREADPOOL_POST_SPINS; i++) {
    wt = w_threads + ((i + n) % nthreads);
    if (pool__mutex_trylock(&wt->mutex) == 0) {
      break;
    } else {
      wt = NULL;
    }
  }

  // fallback to pessimistic mode
  if (wt == NULL) {
    i = n % nthreads;
    wt = w_threads + i;
    pool__mutex_lock(&wt->mutex);
  }

  QUEUE_INSERT_TAIL(&wt->queue, q);
  if (wt->idle) {
    pool__cond_signal(&wt->cond);
  }
  pool__mutex_unlock(&wt->mutex);
}


static void pool_init(void) {
  unsigned int i;
  struct w_thread_s* wt;
  sem_t sem;
  struct w_thread_args_s* args;

  for (wt = w_threads; wt < w_threads + nthreads; wt++) {
    if (pool__cond_init(&wt->cond))
      abort();
    if (pool__mutex_init(&wt->mutex))
      abort();
    QUEUE_INIT(&wt->queue);
  }

  if (sem_init(&sem, 0, 0))
    abort();

  args = malloc(nthreads * sizeof(struct w_thread_args_s));
  for (i = 0; i < nthreads; i++) {
    (args + i)->sem = &sem;
    (args + i)->n = i;
    wt = w_threads + i;
    wt->idle = 0;
    if (pool__thread_create(&wt->thread, pool__worker, args + i))
      abort();
  }

  for (i = 0; i < nthreads; i++)
    pool__sem_wait(&sem);

  free(args);
  pool__sem_destroy(&sem);
}


void pool_submit_work(struct work_s* w,
                      void (*work)(struct work_s* w)) {
  w->work = work;
  pool__post(&w->wq);
}


void pool_wait_all(void) {
  struct w_thread_s* wt;

  // post exit message into all queues
  for (wt = w_threads; wt < w_threads + nthreads; wt++) {
    pool__mutex_lock(&wt->mutex);
    QUEUE_INSERT_TAIL(&wt->queue, &exit_message);
    if (wt->idle) {
      pool__cond_signal(&wt->cond);
    }
    pool__mutex_unlock(&wt->mutex);
  }

  for (wt = w_threads; wt < w_threads + nthreads; wt++)
    pool__thread_join(&wt->thread);
}

/* threadpool end */

/* benchmark start */

#define TOTAL_CALLS 10000000
#define MAX_PRIME 100

static struct work_s req[TOTAL_CALLS];

/* Emulates short CPU intensive task by calculating
 * prime numbers count for a given upper limit.
 */
static void process_task(struct work_s* req) {
  unsigned int i, n, limit, primes = 0;

  limit = req->res % MAX_PRIME;
  for (n = 1; n <= limit; ++n)
    for (i = 2; (i <= n) && (n % i != 0); ++i);
      if (i == n)
        ++primes;

  req->res = primes;
}

static uint64_t get_posix_clock_time() {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0)
    return ts.tv_sec * (uint64_t) 1e9 + ts.tv_nsec;
  else
    return 0;
}

int main() {
  int i;
  uint64_t t_start, t_end;
  double t_diff_sec;

  pool_init();
  for (i = 0; i < TOTAL_CALLS; i++) {
    req[i].res = i;
  }

  t_start = get_posix_clock_time();
  for (i = 0; i < TOTAL_CALLS; i++) {
    pool_submit_work(&req[i], process_task);
  }
  pool_wait_all();
  t_end = get_posix_clock_time();

  t_diff_sec = (double) (t_end - t_start) / 1000000000.0;
  printf("threadpool: %.0f req/s, total time %.2f secs\n",
         (double) TOTAL_CALLS / t_diff_sec, t_diff_sec);

  return 0;
}

/* benchmark end */
