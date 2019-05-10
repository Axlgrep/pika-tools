// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string.h>

#include <algorithm>
#include <fstream>
#include <iostream>

#include "slash/include/slash_status.h"
#include "slash/include/slash_string.h"
#include "include/pika_binlog.h"

std::string db_dump_path;
int32_t db_dump_filenum;
int64_t db_dump_offset;
std::string new_pika_log_path;

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
  std::cout << "New Pika log_path:     " << new_pika_log_path << std::endl;
  std::cout << "========================================================" << std::endl;
  std::cout << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  -d   -- db dump path (required)" << std::endl;
  std::cout << "  -l   -- new pika log_path (required)" << std::endl;
  std::cout << "  example: ./auto_db_sync -d /data1/pika_old/dump/20190508/ -l /data01/pika_new/log/db0" << std::endl;
}

int main(int argc, char* argv[]) {
  int opt;
  while ((opt = getopt(argc, argv, "d:l:")) != -1) {
    switch (opt) {
      case 'd' :
        db_dump_path = std::string(optarg);
        break;
      case 'l' :
        new_pika_log_path = std::string(optarg);
        break;
      default :
        Usage();
        exit(-1);
    }
  }
  if (db_dump_path.empty() || new_pika_log_path.empty()) {
    Usage();
    exit(-1);
  }
  if (db_dump_path.back() != '/') {
     db_dump_path.push_back('/');
  }
  std::size_t found = db_dump_path.find("dump");
  if (found != std::string::npos) {
    if (db_dump_path.size() - 1 - found == 4) {
      // input db path like: /data1/pika111/dump/
      std::cout << "Choose a db dump under: " << db_dump_path << std::endl;
      exit(-1);
    }
  } else  {
    std::cout << "Db dump path invalid" << std::endl;
    exit(-1);
  }

  if (new_pika_log_path.back() != '/') {
    new_pika_log_path.push_back('/');
  }
  // if this dir exist
  if (slash::IsDir(new_pika_log_path) == 0) {
    std::cout << "Dir " << new_pika_log_path << "exist, please delete it!" << std::endl;
    exit(-1);
  }

  PrintInfo();
  std::cout << std::endl << "Step 1, Parse Info file from " << db_dump_path << std::endl;
  ParseInfoFile(db_dump_path);

  std::cout << std::endl << "Step 2, Generate manifest file to " << new_pika_log_path << std::endl;
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
