//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "chrono"
#include "ctime"
#include "iostream"

#include "stdlib.h"
#include "unistd.h"
#include "stdint.h"
#include "string.h"

#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"

#include "utils.h"
#include "binlog_consumer.h"


std::string ip = "127.0.0.1";
uint32_t port = 6379;
std::string pass_wd;
std::string binlog_path = "./log";
std::string files_to_send = "0";
int64_t file_offset = 0;

void PrintInfo(const std::time_t& now) {
  std::cout << "================== Binlog Sender==================" << std::endl;
  std::cout << "Ip : " << ip << std::endl;
  std::cout << "Port : " << port << std::endl;
  std::cout << "Password : " << pass_wd << std::endl;
  std::cout << "Binlog_path: " << binlog_path << std::endl;
  std::cout << "Files_to_send: " << files_to_send << std::endl;
  std::cout << "File_offset : " << file_offset << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "-h    -- displays this help information and exits" << std::endl;
  std::cout << "-n    -- input binlog path" << std::endl;
  std::cout << "-f    -- files to send, default = 0" << std::endl;
  std::cout << "-o    -- the offset that the first file starts sending" << std::endl;
  std::cout << "-i    -- ip of the pika server" << std::endl;
  std::cout << "-p    -- port of the pika server" << std::endl;
  std::cout << "-a    -- passwork of the pika server" << std::endl;
}

int main(int argc, char *argv[]) {

  int32_t opt;
  bool need_auth = false;
  while ((opt = getopt(argc, argv, "hi:p:a:n:f:o:")) != -1) {
    switch (opt) {
      case 'h' :
        Usage();
        exit(0);
      case 'i' :
        ip = optarg;
        break;
      case 'p' :
        port = std::atoi(optarg);
        break;
      case 'a' :
        need_auth = true;
        pass_wd = optarg;
        break;
      case 'n' :
        if (!binlog_path.empty() && binlog_path.back() != '/') {
          binlog_path += "/";
        }
        break;
      case 'f' :
        files_to_send = optarg;
        break;
      case 'o' :
        file_offset = std::atoi(optarg);
        break;
      default:
        break;
    }
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  std::vector<uint32_t> files;
  GetFileList(files_to_send, &files);
  if (!CheckBinlogSequential(files)) {
    fprintf (stderr, "Please input sequential binlog num, exit...\n");
    exit(-1);
  }

  int32_t first_index = 0;
  int32_t last_index = files.size() - 1;

  BinlogConsumer* binlog_consumer =
    new BinlogConsumer(binlog_path, files[first_index], file_offset, files[last_index]);

  if (!binlog_consumer->Init()) {
    fprintf(stderr, "Binlog comsumer initialization failure, exit...\n");
    exit(-1);
  } else if (!binlog_consumer->trim()) {
    fprintf(stderr, "Binlog comsumer trim failure, maybe the offset is illegal, exit...\n");
    exit(-1);
  }

  pink::PinkCli *cli = pink::NewRedisCli();
  cli->set_connect_timeout(3000);
  fprintf (stderr, "Connecting...\n");
  Status pink_s = cli->Connect(ip, port, "");
  if (!pink_s.ok()) {
    fprintf (stderr, "Connect failed, %s, exit...\n", pink_s.ToString().c_str());
    exit(-1);
  }
  fprintf (stderr, "Connected...\n");

  if (need_auth) {
    fprintf (stderr, "Send Auth...\n");
    std::string ok = "ok";
    std::string auth_cmd;
    pink::RedisCmdArgsType argv;
    argv.push_back("auth");
    argv.push_back(pass_wd);
    pink::SerializeRedisCommand(argv, &auth_cmd);

    pink_s = cli->Send(&auth_cmd);
    pink_s = cli->Recv(&argv);
    if (argv.size() == 1 && !strcasecmp(argv[0].c_str(), ok.c_str())) {
      fprintf (stderr, "Auth success...\n");
    } else {
      fprintf (stderr, "Auth failed...\n");
      exit(-1);
    }
  }

  while (true) {
    std::string scratch;
    slash::Status s = binlog_consumer->Parse(&scratch);
    if (s.ok()) {
      printf("scratch size = %d\n", scratch.size());
    } else if (s.IsComplete()) {
      printf("finish\n");
      break;
    } else {
      fprintf (stderr, "Binlog Parse err: %s, exit...\n", s.ToString().c_str());
      exit(-1);
    }
  }

  delete binlog_consumer;
  delete cli;
  return 0;
}

