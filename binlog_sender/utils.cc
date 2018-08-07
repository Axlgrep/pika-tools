//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "utils.h"

bool Exists(const std::string& base, const std::string& pattern) {
  std::string::size_type n;
  n = base.find(pattern);
  if (n == std::string::npos) {
    return false;
  } else {
    return true;
  }
}

void GetFileList(const std::string& files_str, std::vector<uint32_t>* files) {
  std::string::size_type pos;
  if (Exists(files_str, COMMA_STR)) {
    std::string str = files_str + COMMA_STR;
    for (uint32_t idx = 0; idx < str.size(); ++idx) {
      pos = str.find(COMMA_STR, idx); 
      if (pos != std::string::npos) {
        std::string file = str.substr(idx, pos - idx);
        files->push_back(atoi(file.data()));
        idx = pos;
      } else {
        break;
      }
    }
  } else if (Exists(files_str, NEG_STR)) {
    pos = files_str.find(NEG_STR);
    uint32_t start_file = atoi(files_str.substr(0, pos).data());
    uint32_t end_file = atoi(files_str.substr(pos + 1).data());
    for (uint32_t file_num = start_file; file_num <= end_file; ++file_num) {
      files->push_back(file_num);
    }
  } else {
    files->push_back(atoi(files_str.data()));
  }
}

bool CheckBinlogSequential(const std::vector<uint32_t>& files) {
  if (files.size() <= 1) {
    return true;
  } else {
    for (uint32_t idx = 0; idx <= files.size() - 2; ++idx) {
      if (files[idx] + 1 != files[idx + 1]) {
        return false;
      }
    }
  }
  return true;
}

bool CheckBinlogExists(const std::string& binlog_path, const std::vector<uint32_t>& files) {
  std::string filename = binlog_path + WRITE2FILE;
  for (int32_t idx = 0; idx < files.size(); ++idx) {
    std::string binlog_file = filename + std::to_string(files[idx]);
    if (!slash::FileExists(binlog_file)) {
      return false;
    }
  }
  return true;
}
