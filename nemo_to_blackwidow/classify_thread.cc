//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "classify_thread.h"

void ClassifyThread::PlusProcessKeyNum() {
  key_num_++;
}

void ClassifyThread::DispatchItem(const std::string& item) {
  do {
    consume_index_ = (consume_index_ + 1) % migrators_.size();
  } while (!migrators_[consume_index_]->LoadItem(item));
}

void* ClassifyThread::ThreadMain() {
  std::string key;
  std::string dst;
  if (type_ == nemo::KV_DB) {
    nemo::KIterator* iter = nemo_db_->KScan("", "", -1, false);
    while (iter->Valid()) {
      EncodeKeyValue(iter->key(), iter->value(), &dst);
      DispatchItem(nemo::DataType::kKv + dst);
      PlusProcessKeyNum();
      iter->Next();
    }
  } else {
    char c_type;
    std::string key_start;
    if (type_ == nemo::HASH_DB) {
      c_type = 'h';
      key_start = nemo::DataType::kHSize;
    } else if (type_ == nemo::LIST_DB) {
      c_type = 'l';
      key_start = nemo::DataType::kLMeta;
    } else if (type_ == nemo::ZSET_DB) {
      c_type = 'z';
      key_start = nemo::DataType::kZSize;
    } else if (type_ == nemo::SET_DB) {
      c_type = 's';
      key_start = nemo::DataType::kSSize;
    } else {
      std::cout << "wrong type of db type in classify thread, exit..." << std::endl;
      exit(-1);
    }
    rocksdb::Iterator* iter = nemo_db_->Scanbytype(c_type);
    iter->Seek(key_start);
    while (iter->Valid() && iter->key().starts_with(key_start)) {
      key = iter->key().ToString();
      iter->Next();
      PlusProcessKeyNum();
      DispatchItem(key);
    }
  }
  is_finish_ = true;
  slash::MutexLock l(&mutex);
  std::cout << "Classify " << type_ << " keys finish, keys num : " << key_num_ << std::endl;
  return NULL;
}
