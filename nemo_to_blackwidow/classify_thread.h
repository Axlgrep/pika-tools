//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CLASSIFY_THREAD_H_
#define INCLUDE_CLASSIFY_THREAD_H_

#include "iostream"
#include "vector"

#include "nemo.h"
#include "pink/include/pink_thread.h"

#include "migrator.h"

class ClassifyThread : public pink::Thread {
  public:
    ClassifyThread(nemo::Nemo* nemo_db, std::vector<Migrator*> migrators, const nemo::DBType& type)
        : nemo_db_(nemo_db), migrators_(migrators), type_(type) {
    }
    virtual ~ClassifyThread() {};
  private:
    virtual void *ThreadMain();
    nemo::Nemo* nemo_db_;
    nemo::DBType type_;
    std::vector<Migrator*> migrators_;
};

#endif  //  INCLUDE_CLASSIFY_THREAD_H_
