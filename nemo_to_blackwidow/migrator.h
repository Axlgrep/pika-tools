//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_MIGRATOR_H_
#define INCLUDE_MIGRATOR_H_

#include "iostream"

#include "blackwidow/blackwidow.h"
#include "pink/include/pink_thread.h"

class Migrator: public pink::Thread {
  public:
    Migrator(nemo::Nemo* nemo_db, blackwidow::BlackWidow* blackwidow_db)
        : nemo_db_(nemo_db),
          blackwidow_db_(blackwidow_db),
          should_exit_(false),
          keys_queue_cond_(&keys_queue_mutex_) {}
    virtual ~Migrator() {}

    void SetShouldExit();
    void LoadKey(const std::string& key);

  private:
    virtual void *ThreadMain();
    nemo::Nemo* nemo_db_;
    blackwidow::BlackWidow* blackwidow_db_;

    std::atomic<bool> should_exit_;
    slash::Mutex keys_queue_mutex_;
    slash::CondVar keys_queue_cond_;
    std::queue<std::string> keys_queue_;
};

#endif  //  INCLUDE_MIGRATOR_H_
