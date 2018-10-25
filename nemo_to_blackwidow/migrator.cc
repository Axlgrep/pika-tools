//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "nemo.h"
#include "utils.h"
#include "migrator.h"

int32_t Migrator::queue_size() {
  slash::MutexLock l(&queue_mutex_);
  return items_queue_.size();
}

void Migrator::PlusMigrateKey() {
  migrate_key_num_++;
}

void Migrator::SetShouldExit() {
  queue_mutex_.Lock();
  should_exit_ = true;
  queue_cond_.Signal();
  queue_mutex_.Unlock();
}

bool Migrator::LoadItem(const std::string& item) {
  queue_mutex_.Lock();
  if (items_queue_.size() >= MAX_QUEUE_SIZE) {
    queue_mutex_.Unlock();
    return false;
  } else {
    items_queue_.push(item);
    queue_cond_.Signal();
    queue_mutex_.Unlock();
    return true;
  }
}

void* Migrator::ThreadMain() {

  char prefix;
  int32_t int32_ret;
  uint64_t uint64_ret;
  std::string item, dst, key, value;
  rocksdb::Status s;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  while (items_queue_.size() || !should_exit_) {

    queue_mutex_.Lock();
    while (items_queue_.empty() && !should_exit_) {
      queue_cond_.Wait();
    }
    queue_mutex_.Unlock();


    if (queue_size() == 0 && should_exit_) {
      return NULL;
    }

    queue_mutex_.Lock();
    item = items_queue_.front();
    items_queue_.pop();
    queue_mutex_.Unlock();


    prefix = item[0];
    if (prefix == nemo::DataType::kKv) {
      dst = item.substr(1);
      DecodeKeyValue(dst, &key, &value);
    } else {
      key = item.substr(1);
    }

    LOG(INFO) << "migrator id: " << migrator_id_ << "  queue size: " << queue_size() << "  type : " << prefix << "  key: " << key;

    if (prefix == nemo::DataType::kKv) {
      blackwidow_db_->Set(key, value);
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

    int64_t ttl = -1;
    nemo_db_->TTL(key, &ttl);
    if (ttl > 0) {
      int64_t timestamp = time(NULL) + ttl;
      blackwidow_db_->Expireat(key, timestamp, &type_status);
    }
    PlusMigrateKey();
  }
  std::cout << "Migrator " << migrator_id_ << " finish, keys num : " << migrate_key_num_ << " exit..." << std::endl;
  return NULL;
}
