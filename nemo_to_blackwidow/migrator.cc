//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "nemo.h"
#include "migrator.h"

int32_t Migrator::queue_size() {
  slash::MutexLock l(&keys_queue_mutex_);
  return keys_queue_.size();
}

void Migrator::PlusMigrateKey() {
  migrate_key_num_++;
}

void Migrator::SetShouldExit() {
  keys_queue_mutex_.Lock();
  should_exit_ = true;
  read_keys_queue_cond_.Signal();
  keys_queue_mutex_.Unlock();
}

void Migrator::LoadKey(const std::string& key) {
  keys_queue_mutex_.Lock();
  while (keys_queue_.size() > MAX_QUEUE_SIZE) {
    write_keys_queue_cond_.Wait();
  }
  keys_queue_.push(key);
  read_keys_queue_cond_.Signal();
  keys_queue_mutex_.Unlock();
}

void* Migrator::ThreadMain() {

  int32_t int32_ret;
  uint64_t uint64_ret;
  rocksdb::Status s;
  while (keys_queue_.size() || !should_exit_) {
    char prefix;
    std::string key;

    keys_queue_mutex_.Lock();
    while (keys_queue_.empty() && !should_exit_) {
      read_keys_queue_cond_.Wait();
    }
    keys_queue_mutex_.Unlock();


    if (queue_size() == 0 && should_exit_) {
      return NULL;
    }

    keys_queue_mutex_.Lock();
    key = keys_queue_.front();
    keys_queue_.pop();
    write_keys_queue_cond_.Signal();
    keys_queue_mutex_.Unlock();

    LOG(INFO) << "migrator id: " << migrator_id_ << ",  queue size: " << queue_size() << ", key: " << key;

    prefix = key[0];
    key = key.substr(1);
    if (prefix == nemo::DataType::kKv) {
      std::string value;
      s = nemo_db_->Get(key, &value);
      if (s.ok()) {
        blackwidow_db_->Set(key, value);
      }
    } else if (prefix == nemo::DataType::kHSize) {
      nemo::HIterator *iter = nemo_db_->HScan(key, "", "", -1, false);
      while (iter->Valid()) {
        blackwidow_db_->HSet(iter->key(), iter->field(), iter->value(), &int32_ret);
        iter->Next();
      }
      delete iter;
    } else if (prefix == nemo::DataType::kLMeta) {
      std::vector<nemo::IV> ivs;
      std::vector<std::string> values;
      int64_t pos = 0;
      int64_t step_length = 500;
      nemo_db_->LRange(key, 0, pos + step_length - 1, ivs);
      while (!ivs.empty()) {
        for (const auto& node : ivs) {
          values.push_back(node.val);
        }
        blackwidow_db_->RPush(key, values, &uint64_ret);

        pos += step_length;
        ivs.clear();
        values.clear();
        nemo_db_->LRange(key, pos, pos + step_length - 1, ivs);
      }
    } else if (prefix == nemo::DataType::kZSize) {
      nemo::ZIterator *iter = nemo_db_->ZScan(key, nemo::ZSET_SCORE_MIN,
              nemo::ZSET_SCORE_MAX, -1, false);
      while (iter->Valid()) {
        blackwidow_db_->ZAdd(iter->key(), {{iter->score(), iter->member()}}, &int32_ret);
        iter->Next();
      }
      delete iter;

    } else if (prefix == nemo::DataType::kSSize) {
      nemo::SIterator *iter = nemo_db_->SScan(key, -1, false);
      while (iter->Valid()) {
        blackwidow_db_->SAdd(iter->key(), {iter->member()}, &int32_ret);
        iter->Next();
      }
      delete iter;
    } else {
      std::cout << "wrong type of db type in migrator, exit..." << std::endl;
      exit(-1);
    }
    PlusMigrateKey();
  }
  LOG(INFO) << "Migrator " << migrator_id_ << " finish, keys num : " << migrate_key_num_ << " exit...";
  return NULL;
}
