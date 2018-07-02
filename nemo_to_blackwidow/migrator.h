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
    Migrator(blackwidow::BlackWidow* blackwidow_db)
        : blackwidow_db_(blackwidow_db) {}
    virtual ~Migrator() {}

  private:
    virtual void *ThreadMain();
    blackwidow::BlackWidow* blackwidow_db_;

    std::queue<std::string> queue_;
};

#endif  //  INCLUDE_MIGRATOR_H_
