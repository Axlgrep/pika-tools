#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <ctime>
#include <chrono>
#include <vector>
#include <iostream>
#include <random>
#include <functional>

#include "hiredis.h"
#include "slash/include/slash_string.h"

#define TIME_OF_LOOP          1000000

using std::default_random_engine;

static int32_t     last_seed = 0;

std::string tables_str = "0";
std::vector<std::string> tables;

std::string hostname  = "127.0.0.1";
int         port      =  9221;
std::string password  = "";
uint32_t    payload_size = 50;
uint32_t    number_of_request = 100000;
uint32_t    thread_num_each_table = 1;

struct ThreadArg{
  pthread_t tid;
  std::string table_name;
  size_t idx;
};

void GenerateRandomString(int32_t len, std::string* target) {
  target->clear();
  char c_map[67] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g',
                    'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                    'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
                    'E', 'F', 'G', 'H', 'I', 'G', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                    'Y', 'Z', '~', '!', '@', '#', '$', '%', '^', '&',
                    '*', '(', ')', '-', '=', '_', '+'};

  default_random_engine e;
  for (int i = 0; i < len; i++) {
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 67;
    target->push_back(c_map[rand_num]);
  }
}

void PrintInfo(const std::time_t& now) {
  std::cout << "=================== Benchmark Client ===================" << std::endl;
  std::cout << "Server host name: " << hostname << std::endl;
  std::cout << "Server port: " << port << std::endl;
  std::cout << "Thread num : " << thread_num_each_table << std::endl;
  std::cout << "Payload size : " << payload_size << std::endl;
  std::cout << "Number of request : " << number_of_request << std::endl;
  std::cout << "Collection of tables: " << tables_str << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tBenchmark_client writes data to the specified table" << std::endl;
  std::cout << "\t-h    -- server hostname (default 127.0.0.1)" << std::endl;
  std::cout << "\t-p    -- server port (default 9221)" << std::endl;
  std::cout << "\t-t    -- thread num of each table (default 1)" << std::endl;
  std::cout << "\t-c    -- collection of table names (default db1)" << std::endl;
  std::cout << "\t-d    -- data size of SET value in bytes (default 50)" << std::endl;
  std::cout << "\t-n    -- number of requests single thread (default 100000)" << std::endl;
  std::cout << "\texample: ./benchmark_client -t 3 -c db1,db2 -d 1024" << std::endl;
}

std::vector<ThreadArg> thread_args;

void *ThreadMain(void* arg) {
  ThreadArg* ta = reinterpret_cast<ThreadArg*>(arg);
  redisContext *c;
  redisReply *res;
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  c = redisConnectWithTimeout(hostname.data(), port, timeout);

  if (c == NULL || c->err) {
    if (c) {
      printf("Thread %lu, Connection error: %s\n", ta->tid, c->errstr);
      redisFree(c);
    } else {
      printf("Thread %lu, Connection error: can't allocate redis context\n", ta->tid);
    }
    return NULL;
  }

  if (!password.empty()) {
    const char* auth_argv[2] = {"AUTH", password.data()};
    size_t auth_argv_len[2] = {4, password.size()};
    res = reinterpret_cast<redisReply*>(redisCommandArgv(c,
                                                         2,
                                                         reinterpret_cast<const char**>(auth_argv),
                                                         reinterpret_cast<const size_t*>(auth_argv_len)));
    if (res == NULL) {
      printf("Thread %lu  Auth Failed, Get reply Error\n", ta->tid);
      freeReplyObject(res);
      redisFree(c);
      return NULL;
    } else {
      if (!strcasecmp(res->str, "OK")) {
      } else {
        printf("Thread %lu Auth Failed: %s, thread exit...\n", ta->idx, res->str);
        freeReplyObject(res);
        redisFree(c);
        return NULL;
      }
    }
    freeReplyObject(res);
  }

  const char* select_argv[2] = {"SELECT", ta->table_name.data()};
  size_t  select_argv_len[2] = {6, ta->table_name.size()};
  res = reinterpret_cast<redisReply*>(redisCommandArgv(c,
                                                       2,
                                                       reinterpret_cast<const char**>(select_argv),
                                                       reinterpret_cast<const size_t*>(select_argv_len)));
  if (res == NULL) {
    printf("Thread %lu Select Table %s Failed, Get reply Error\n", ta->tid, ta->table_name.data());
    freeReplyObject(res);
    redisFree(c);
    return NULL;
  } else {
    if (!strcasecmp(res->str, "OK")) {
      printf("Table %s Thread %lu Select DB Success, start to write data...\n", ta->table_name.data(), ta->idx);
    } else {
      printf("Table %s Thread %lu Select DB Failed: %s, thread exit...\n", ta->table_name.data(), ta->idx, res->str);
      freeReplyObject(res);
      redisFree(c);
      return NULL;
    }
  }
  freeReplyObject(res);

  for (size_t idx = 0; idx < number_of_request; ++idx) {
    const char* set_argv[3];
    size_t set_argvlen[3];
    std::string key;
    std::string value;
    GenerateRandomString(10, &key);
    GenerateRandomString(payload_size, &value);

    set_argv[0] = "set"; set_argvlen[0] = 3;
    set_argv[1] = key.c_str(); set_argvlen[1] = key.size();
    set_argv[2] = value.c_str(); set_argvlen[2] = value.size();

    res = reinterpret_cast<redisReply*>(redisCommandArgv(c,
                                                         3,
                                                         reinterpret_cast<const char**>(set_argv),
                                                         reinterpret_cast<const size_t*>(set_argvlen)));
   if (res == NULL || strcasecmp(res->str, "OK")) {
     printf("Table %s, Thread %lu Exec command error: %s, thread exit...\n", ta->table_name.data(), ta->idx, res != NULL ? res->str : "");
     freeReplyObject(res);
     redisFree(c);
     return NULL;
   }
   freeReplyObject(res);
  }
  redisFree(c);
  return NULL;
}

// ./benchmark_client
// ./benchmark_client -h
// ./benchmark_client -b db1:5:10000,db2:3:10000
int main(int argc, char *argv[]) {
  int opt;
  while ((opt = getopt(argc, argv, "h:p:a:t:c:d:n:")) != -1) {
    switch (opt) {
      case 'h' :
        hostname = std::string(optarg);
        break;
      case 'p' :
        port = atoi(optarg);
        break;
      case 'a' :
        password = std::string(optarg);
        break;
      case 't' :
        thread_num_each_table = atoi(optarg);
        break;
      case 'c' :
        tables_str = std::string(optarg);
        break;
      case 'd' :
        payload_size = atoi(optarg);
        break;
      case 'n' :
        number_of_request = atoi(optarg);
        break;
      default :
        Usage();
        exit(-1);
    }
  }

  slash::StringSplit(tables_str, ',', tables);

  if (tables.empty()) {
    Usage();
    exit(-1);
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  for (const auto& table : tables) {
    for (size_t idx = 0; idx < thread_num_each_table; ++idx) {
      thread_args.push_back({0, table, idx});
    }
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_create(&thread_args[idx].tid, NULL, ThreadMain, &thread_args[idx]);
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_join(thread_args[idx].tid, NULL);
  }

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : "
            << hours << " hours "
            << minutes % 60 << " minutes "
            << seconds % 60 << " seconds "
            << std::endl;
  return 0;
}
