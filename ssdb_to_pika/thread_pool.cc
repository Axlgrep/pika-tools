#include <assert.h>

#include "thread_pool.h"

ThreadPool::ThreadPool(int initial_size) : worker_size_(initial_size), is_started_(false) {
  Start();
}

void ThreadPool::Start() {
  assert(workthreads_.empty());
  is_started_ = true;
  workthreads_.reserve(worker_size_);
  for (int i = 0; i < worker_size_; ++i) {
    workthreads_.emplace_back(new std::thread(std::bind(&ThreadPool::ThreadLoop, this)));
  }
}

ThreadPool::~ThreadPool() {
  if (is_started_) {
    Stop();
  }
}

void ThreadPool::Stop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!taskque_.empty() && is_started_) {
      cond_.wait(lock); 
    }
    is_started_ = false;
    cond_.notify_all();
  }
  for (auto iter = workthreads_.begin(); iter != workthreads_.end(); ++iter) {
    (*iter)->join();
  }
  workthreads_.clear();
}

void ThreadPool::ThreadLoop() {
  while (is_started_) {
    Task task = Take();
    if (task) {
      task();
    }
  }
}

void ThreadPool::AddTask(const Task& tsk) {
  std::unique_lock<std::mutex> lock(mutex_);  
  taskque_.emplace_back(std::move(tsk));
  cond_.notify_one();
}

ThreadPool::Task ThreadPool::Take() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (taskque_.empty() && is_started_) {
    cond_.wait(lock);
  }
  Task task;
  TaskQue::size_type size = taskque_.size();
  if (!taskque_.empty() && is_started_) {
    task = taskque_.front();
    taskque_.pop_front();
    assert(size - 1 == taskque_.size());
  }
  return task;
}

