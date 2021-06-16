/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <getopt.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <execinfo.h>
#include <unistd.h>
#include <string.h>
#include <openssl/sha.h>
#include <signal.h>
#include <inttypes.h>
#include <pthread.h>

#include "conc.h"
#include "table.h"
#include "db.h"
#include "debug.h"
#include "generator.h"

// one for each thread
struct DBParams {
  char * tag;
  uint64_t vlen;
  char * meta_dir;
  char * cm_conf_fn;
  uint64_t nr_threads;
  uint64_t p_writer;
  char * generator;
  uint64_t range;
  uint64_t sec; // run time
  uint64_t nr_report;
};

static const uint64_t nr_configs = 1;

// Params Array
// Cases Array
static struct DBParams pstable[] = {
  //tag    vlen  meta_dir       cm_conf_fn     threads  pw(p_writer)   gen        range                     sec   nr
  {"Dummy", 100, "lsmtrie_tmp", "cm_conf1.txt", 1,      100,           "uniform", UINT64_C(0x100000000000), 3000, 100000},
};

// singleton
struct TestState {
  uint64_t token;      // 线程计数器
  uint64_t nr_100;     // 每 100 次操作计数一次
  uint64_t usec_last;  // 上一次统计结束时间
  uint64_t usec_start; // case 运行开始时间
  bool test_running; // 指令指示运行状态
  pthread_mutex_t test_lock;
  struct GenInfo *gi; // 负载生成器
  struct DB * db;     // db 实例
  uint32_t * latency; // 延迟
  uint8_t buf[BARREL_ALIGN]; // 4KB BUFFFFER
};

static struct TestState __ts = {0,0,0,0,false,PTHREAD_MUTEX_INITIALIZER,NULL,NULL,NULL,{0,},};

  static void
show_dbparams(const struct DBParams * const ps)
{
  printf("MIX_TEST: %s\n", ps->tag);
  printf("    -v #vlen:       %lu\n", ps->vlen);
  printf("    -d #dir:        %s\n",          ps->meta_dir);
  printf("    -c #cm_conf_fn: %s\n",          ps->cm_conf_fn);
  printf("    -a #nr_threads: %lu\n", ps->nr_threads);
  printf("    -w #p_writer:   %lu\n", ps->p_writer);
  printf("    -g #generator:  %s\n",          ps->generator);
  printf("    -r #range:      %lu\n", ps->range);
  printf("    -t #sec:        %lu\n", ps->sec);
  printf("    -n #nr_report:  %lu\n", ps->nr_report);
  fflush(stdout);
}

  static void
mixed_worker(const struct DBParams * const ps)
{
  uint64_t keys[100];

  // Max p_writer = 100
  assert(ps->p_writer <= 100u);

  // 分配对应的 KV 空间
  struct KeyValue kvs[100] __attribute__((aligned(8)));
  for (uint64_t i = 0; i < 100u; i++) {
    kvs[i].klen = sizeof(keys[i]);
    kvs[i].pk   = (typeof(kvs[i].pk))(&(keys[i]));
    kvs[i].vlen = ps->vlen;
    kvs[i].pv   = __ts.buf;
  }

  // wait for instruction
  // 只有 test_running 为 true 时才能运行
  for (;;) {
    if (__ts.test_running == true) break;
    usleep(100);
  }

  // loop
  // 如果没有收到终止信号，该程序将一直运行
  while (__ts.test_running) {
    // random keys
    // 生成随机的 Key
    for (uint64_t i = 0; i < 100u; i++) {
      const uint64_t rkey = __ts.gi->next(__ts.gi);
      keys[i] = rkey;
    }

    // write items
    // 批量插入数据
    if (ps->p_writer > 0) {
      const bool r = db_multi_insert(__ts.db, ps->p_writer, kvs);
      assert(r);
    }

    // read keys
    // p_writer < 100u 时进行查询操作
    // 当写入的一批请求个数小于 100 时，用读请求来填充至 100 次操作
    for (uint64_t i = ps->p_writer; i < 100u; i++) {

      // 计算延迟
      const uint64_t t0 = debug_time_usec();
      struct KeyValue * const kv = db_lookup(__ts.db, sizeof(keys[i]), (const uint8_t *)(&(keys[i])));
      const uint64_t t1 = debug_time_usec();

      // 对该延迟下的计数器 + 1
      latency_record(t1 - t0, __ts.latency);
      if (kv) {
        free(kv);
      }
    }

    // __ts.nr_100 计数 + 1
    // 累计执行了 nr_100 * 100u 次操作（读写混合，如果 writer 为 100，就只有写）
    const uint64_t nr_100 = __sync_add_and_fetch(&(__ts.nr_100), 1);

    // 输出统计信息
    if ((nr_100 % ps->nr_report) == 0) {
      pthread_mutex_lock((&__ts.test_lock));
      {
        // 获取当前时间
        const uint64_t usec = debug_time_usec();

        // 距离上一次统计的时间，delta
        const double udiff = (usec - __ts.usec_last) / 1000000.0;

        // 从开始运行到现在的总时间
        const double elapsed = (usec - __ts.usec_start) / 1000000.0;

        // 该段时间内执行的操作数 / 该段时间 = 该段时间的 QPS
        const double qps = ((double)ps->nr_report) * 100.0 / udiff;

        // ops 为整个 case 的总操作数
        printf("@@ ops %14lu time %12lf delta %12lf qps %12.2lf\n", nr_100 * 100u, elapsed, udiff, qps);

        // 更新最近一次 report 结束的计时器
        __ts.usec_last = usec;

        // 输出 DB 状态
        db_stat_show(__ts.db, stdout);

        // flush stdout
        fflush(stdout);
      }
      pthread_mutex_unlock(&(__ts.test_lock));
    }
  }
}

  static void *
mixed_thread(void *p)
{
  // 每创建一个线程，计数器 __ts.token + 1
  const uint64_t token = __sync_fetch_and_add(&(__ts.token), 1u);
  // 根据 token 绑核
  conc_set_affinity_n(token % 8);
  // 构造相应的 worker，执行相应的操作
  mixed_worker((struct DBParams *)p);

  // 执行完成后线程退出
  pthread_exit(NULL);
  return NULL;
}

  static struct GenInfo *
gen_initial(const char * const name, const uint64_t range)
{
  // 为指定参数则生成 uniform 负载
  //static const uint64_t range = UINT64_C(0x20000000000);
  if (name == NULL) {
    return generator_new_uniform(0, range);
  }

  // 针对不同的参数设定，生成指定范围内的数据分布
  const int len = strlen(name);
  if (0 == strncmp(name, "counter", len)) {
    return generator_new_counter(0);
  } else if (0 == strncmp(name, "exponential", len)) {
    return generator_new_exponential(95.0, (double)range);
  } else if (0 == strncmp(name, "zipfian", len)) {
    return generator_new_zipfian(0, range);
  } else if (0 == strncmp(name, "xzipfian", len)) {
    return generator_new_xzipfian(0, range);
  } else if (0 == strncmp(name, "uniform", len)) {
    return generator_new_uniform(0, range);
  } else {
    return generator_new_uniform(0, range);
  }
}

  static uint64_t
wait_for_deadline(const uint64_t sec)
{
  printf("######## Start\n");
  sleep(1);
  const uint64_t dur = sec * 1000000u;
  const uint64_t start = debug_time_usec();
  __ts.usec_start = start;
  __ts.usec_last = start;

  // 判断运行时间是否超时，超时则将 test_running 设置为 false
  __ts.test_running = true;
  while(__ts.test_running == true) {
    sleep(1);
    const uint64_t now = debug_time_usec();
    if (now - start > dur) break;
  }
  __ts.test_running = false;

  // 返回从开始到完全结束的总的执行时间
  const uint64_t finish = debug_time_usec();
  printf("######## Finish\n");
  return finish - start;
}

// 接收到中断信号就将 test_running 置为 false 来结束负载执行
  static void
sig_handler_int(const int sig)
{
  (void)sig;
  __ts.test_running = false;
}

  static void
sig_handler_dump(const int sig)
{
  (void)sig;
  db_force_dump_meta(__ts.db);
  debug_trace();
}

  static void
sig_install_all(void)
{
  struct sigaction sa;
  // 初始化信号集合 sa_mask ，将 sa_mask 设置为空
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART;

  // 忽略的处理方式，忽略信号
  sa.sa_handler = SIG_IGN;
  // 检查或修改与指定信号 SIGHUP 相关联的处理动作
  // 本信号在用户终端连接(正常或非正常)结束时发出, 通常是在终端的控制进程结束时, 
  // 通知同一session内的各个作业, 这时它们与控制终端不再关联。
  sigaction(SIGHUP, &sa, NULL);
  // SIGALRM 时钟定时信号, 计算的是实际的时间或时钟时间. alarm函数使用该信号.
  sigaction(SIGALRM, &sa, NULL);

  // 中断
  sa.sa_handler = sig_handler_int;
  // SIGINT 程序终止(interrupt)信号, 在用户键入INTR字符(通常是Ctrl-C)时发出，用于通知前台进程组终止进程
  sigaction(SIGINT, &sa, NULL);
  // SIGTERM 程序结束(terminate)信号, 与SIGKILL不同的是该信号可以被阻塞和处理。
  // 通常用来要求程序自己正常退出，shell命令kill缺省产生这个信号。如果进程终止不了，我们才会尝试SIGKILL
  sigaction(SIGTERM, &sa, NULL);
  // SIGQUIT 和SIGINT类似, 但由QUIT字符(通常是Ctrl-\)来控制. 进程在因收到SIGQUIT退出时会产生core文件, 
  // 在这个意义上类似于一个程序错误信号。
  sigaction(SIGQUIT, &sa, NULL);

  // 强制 dump
  sa.sa_handler = sig_handler_dump;
  // SIGUSR1 留给用户使用
  sigaction(SIGUSR1, &sa, NULL);
}

  static void
mixed_test(const struct DBParams * const p)
{
  // 初始化相关信号的处理
  sig_install_all();
  // 根据当前时间产生随机数种子
  srandom(debug_time_usec());
  // 输出当前 DB 使用的参数
  show_dbparams(p);
  // 生成负载初始化，对应不同的分布和不同的范围
  __ts.gi = gen_initial(p->generator, p->range);

  // 根据参数读取或创建 DB，初始化 DB
  __ts.db = db_touch(p->meta_dir, p->cm_conf_fn);
  assert(__ts.db);
  // 初始化 4KB Buffer
  memset(__ts.buf, 0x5au, BARREL_ALIGN);

  // 为存储延迟的变量分配内存
  __ts.latency = latency_initial();

  // 使用的线程数
  const uint64_t nth = p->nr_threads;
  pthread_t pth[nth];
  // 初始化线程属性
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  // 设置线程分离状态
  // 线程的分离状态决定一个线程以什么样的方式来终止自己。在默认情况下线程是非分离状态的，这种情况下，原有的线程等待创建的线程结束。
  // pthread_join（）函数返回时，创建的线程才算终止，才能释放自己占用的系统资源。
  // 而分离线程不是这样子的，它没有被其他的线程所等待，自己运行结束了，线程也就终止了，马上释放系统资源。
  // PTHREAD _CREATE_JOINABLE 非分离线程
  // PTHREAD_CREATE_DETACHED 分离线程
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  // 创建相应的 worker 线程执行负载
  // 这里其实时客户端线程，注意和 DB 的内部线程区分。
  for (uint64_t i = 0; i < nth; i++) {
    const int rc = pthread_create(&(pth[i]), &attr, mixed_thread, (void *)p);
    assert(rc == 0);
    pthread_setname_np(pth[i], "Worker");
  }

  // 等待执行时间结束
  const uint64_t dur = wait_for_deadline(p->sec);

  // 等待客户端线程结束
  for (uint64_t i = 0; i < nth; i++) {
    pthread_join(pth[i], NULL);
  }

  // 输出总的执行的操作数 Ops
  printf("Op %lu\n", __ts.nr_100 * 100u);
  // 输出本轮实质花费的时间
  printf("time_usec %lu\n", dur);
  // 输出本轮的 QPS
  printf("QPS %.4lf\n", ((double)__ts.nr_100) * 100000000.0 / ((double)dur));

  // 销毁负载生成器
  generator_destroy(__ts.gi);
  __ts.gi = NULL;

  // 输出 DB 状态
  db_stat_show(__ts.db, stdout);
  // 输出 GET 操作的延迟分布情况
  latency_show("GET", __ts.latency, stdout);
  // 释放延迟变量
  free(__ts.latency);
  // 强制刷新输出
  fflush(stdout);
  // 关闭 DB
  db_close(__ts.db);
}

  int
main(int argc, char ** argv)
{
  printf("!Mixed Test: Compiled at %s %s\n", __DATE__, __TIME__);
  int opt;
  // default opts
  struct DBParams ps = pstable[0]; // see default operations above
  while ((opt = getopt(argc, argv,
          "x:" // test case id (any number you like) if set case id > 1, should add param array in pstable.
          "v:" // constant value size
          "a:" // nr_threads (user threads)
          "w:" // p_writers 0 to 100 （其实就是一次 multi_insert 批量插入的数量）
          "t:" // seconds for each round 每轮的超时时间，执行时间
          "n:" // nr_report: report stats every 100n operations
          "r:" // range of gen
          "d:" // meta dir: either load existing db or create new db
          "c:" // cm_conf_fn: the stroage config file
          "g:" // generator c,e,z,x,u
          "h"  // help
          "l"  // list pre-defined params
          )) != -1) {
    switch(opt) { 
      case 'x': {
                  const uint64_t id = strtoull(optarg, NULL, 10);
                  if (id < nr_configs) ps = pstable[id];
                  break;
                }
      case 'v': ps.vlen       = strtoull(optarg, NULL, 10); break;
      case 'a': ps.nr_threads = strtoull(optarg, NULL, 10); break;
      case 'w': ps.p_writer   = strtoull(optarg, NULL, 10); break;
      case 't': ps.sec        = strtoull(optarg, NULL, 10); break;
      case 'n': ps.nr_report  = strtoull(optarg, NULL, 10); break;
      case 'r': ps.range      = strtoull(optarg, NULL, 10); break;

      case 'd': ps.meta_dir   = strdup(optarg); break;
      case 'c': ps.cm_conf_fn = strdup(optarg); break;
      case 'g': ps.generator  = strdup(optarg); break;
      case 'h': { show_dbparams(&ps); exit(1); }
      case 'l': {
                  for (uint64_t i = 0; i < nr_configs; i++) {
                    printf("====param %lu====\n", i);
                    show_dbparams(&(pstable[i]));
                  }
                  exit(1);
                }
      case '?': perror("arg error\n"); exit(1);
      default : perror("arg error\n"); exit(1);
    }
  }
  mixed_test(&ps);
  return 0;
}
