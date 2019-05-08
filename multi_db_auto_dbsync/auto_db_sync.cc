// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string.h>

#include <algorithm>
#include <fstream>
#include <iostream>

#include "slash/include/slash_status.h"
#include "include/pika_binlog.h"
#include "include/pika_conf.h"

PikaConf* g_pika_conf;
std::string db_dump_path;
int32_t db_dump_filenum;
int64_t db_dump_offset;
std::string new_pika_db_path;
std::string new_pika_log_path;
std::string config_file;

void PikaConfInit(const std::string& path) {
  g_pika_conf = new PikaConf(path);
  if (g_pika_conf->Load() != 0) {
    std::cout << "Load conf error" << std::endl;
    exit(-1);
  }
}

void ParseInfoFile(const std::string& path) {
  std::string info_file = path + kBgsaveInfoFile;
  if (!slash::FileExists(info_file)) {
    std::cout << "Info file " << info_file << " does not exist" << std::endl;
    exit(-1);
  }

  // Got new binlog offset
  std::ifstream is(info_file);
  if (!is) {
    std::cout << "Failed to open info file " << info_file;
    exit(-1);
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        std::cout << "Format of info file " << info_file << " error, line : " << line;
        is.close();
        exit(-1);
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      std::cout << "Format of info file " << info_file << " error, line : " << line;
      is.close();
      exit(-1);
    }
  }
  is.close();

  db_dump_filenum = filenum;
  db_dump_offset = offset;
  std::cout << "Information from info_file " << info_file << std::endl
      << "  db_dump_ip: " << master_ip << std::endl
      << "  db_dump_port: " << master_port << std::endl
      << "  filenum: " << filenum << std::endl
      << "  offset: " << offset << std::endl;
}

void PrintInfo() {
  std::cout << std::endl;
  std::cout << "==================== Configuration =====================" << std::endl;
  std::cout << "Db dump path:          " << db_dump_path << std::endl;
  std::cout << "NEW pika config file:  " << config_file << std::endl;
  std::cout << "========================================================" << std::endl;
  std::cout << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  -d   -- db dump path (required)" << std::endl;
  std::cout << "  -c   -- NEW pika config file (required)" << std::endl;
  std::cout << "  example: ./auto_db_sync -s /data1/pika111/dump/20190508/ -c /data1/pika222/pika.conf" << std::endl;
}


int main(int argc, char* argv[]) {
  int opt;
  while ((opt = getopt(argc, argv, "d:c:")) != -1) {
    switch (opt) {
      case 'd' :
        db_dump_path = std::string(optarg);
        break;
      case 'c' :
        config_file = std::string(optarg);
        break;
      default :
        Usage();
        exit(-1);
    }
  }
  if (db_dump_path.empty() || config_file.empty()) {
    Usage();
    exit(-1);
  }
  if (db_dump_path.back() != '/') {
     db_dump_path.push_back('/');
  }
  std::size_t found = db_dump_path.find("dump");
  if (found != std::string::npos) {
    if (db_dump_path.size() - found == 4) {
      // input db path like: /data1/pika111/dump/
      std::cout << "Choose a db dump under: " << db_dump_path << std::endl;
      exit(-1);
    }
  } else  {
    std::cout << "Db dump path invalid" << std::endl;
    exit(-1);
  }

  PrintInfo();
  std::cout << std::endl << "Step 1, Parse Info file from " << db_dump_path << std::endl;
  ParseInfoFile(db_dump_path);

  std::cout << std::endl << "Step 2, Load Conf File from " << config_file << std::endl;
  PikaConfInit(config_file);

  std::cout << std::endl << "Step 3, Create New Pika db path." << std::endl;
  // create new pika db path
  if (g_pika_conf->db_path().back() == '/') {
    new_pika_db_path = g_pika_conf->db_path() + "db0/";
  } else {
    new_pika_db_path = g_pika_conf->db_path() + "/db0/";
  }

  int res = slash::CreatePath(new_pika_db_path);
  if (res) {
    std::cout << "CreatePath " << new_pika_db_path << " failed" << std::endl;
    exit(-1);
  }
  std::cout << "  " << new_pika_db_path << std::endl;

  std::cout << std::endl << "Step 4, Create New Pika log path." << std::endl;
  // create new pika log path
  if (g_pika_conf->log_path().back() == '/') {
    new_pika_log_path = g_pika_conf->log_path() + "db0/";
  } else {
    new_pika_log_path = g_pika_conf->log_path() + "/db0/";
  }
  res = slash::CreatePath(new_pika_log_path);
  if (res) {
    std::cout << "CreatePath " << new_pika_log_path << " failed" << std::endl;
    exit(-1);
  }
  std::cout << "  " << new_pika_log_path << std::endl;

  std::cout << std::endl << "Step 5, Copy from " << db_dump_path << " to " << new_pika_db_path << std::endl;
  // copy db dump to new pika db path
  std::string copy_cmd = "cp -r " + db_dump_path + "*" + " " + new_pika_db_path;
  std::cout << "command: " << copy_cmd << std::endl;
  while (1) {
    std::cout << "Confirm: Y/N" << std::endl;
    std::string confirm;
    std::cin >> confirm;
    std::transform(confirm.begin(), confirm.end(), confirm.begin(), ::tolower);
    if (confirm == "y" || confirm == "yes") {
      break;
    } else if (confirm == "n" || confirm == "no") {
      exit(-1);
    }
  }
  system(copy_cmd.c_str());

  std::cout << std::endl << "Step 5, Generate manifest file to " << new_pika_log_path << std::endl;
  // generate manifest and newest binlog
  Binlog binlog(new_pika_log_path);
  slash::Status s = binlog.SetProducerStatus(db_dump_filenum, db_dump_offset);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }
  std::cout << std::endl << "DB Sync done ! Try Incremental Sync Maybe." << std::endl;
  return 0;
}
