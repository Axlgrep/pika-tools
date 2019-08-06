#include <assert.h>
#include <iostream>

#include "ThreadPool.h"

ThreadPool::ThreadPool(int initial_size) : m_size(initial_size), m_is_start(false) {
  Start();
}

void ThreadPool::Start() {
  assert(m_workthreads.empty());
  m_is_start = true;
  m_workthreads.reserve(m_size);
  for (int i = 0; i < m_size; ++i) {
    m_workthreads.emplace_back(new std::thread(std::bind(&ThreadPool::ThreadLoop, this)));
  }
}

ThreadPool::~ThreadPool() {
  if (m_is_start) {
    Stop();
  }
}

void ThreadPool::Stop() {
  {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (!m_taskque.empty() && m_is_start) {
      m_cond.wait(lock); 
    }
    m_is_start = false;
    m_cond.notify_all();
  }
  for (auto iter = m_workthreads.begin(); iter != m_workthreads.end(); ++iter) {
    (*iter)->join();
    delete *iter;
  }
  m_workthreads.clear();
}

void ThreadPool::ThreadLoop() {
  while (m_is_start) {
    task_t task = Take();
    if (task) {
      task();
    }
  }
}

void ThreadPool::AddTask(const task_t& tsk) {
  std::unique_lock<std::mutex> lock(m_mutex);  
  m_taskque.emplace_back(std::move(tsk));
  m_cond.notify_one();
}

ThreadPool::task_t ThreadPool::Take() {
  std::unique_lock<std::mutex> lock(m_mutex);
  while (m_taskque.empty() && m_is_start) {
    m_cond.wait(lock);
  }
  task_t tsk;
  TaskQue::size_type size = m_taskque.size();
  if (!m_taskque.empty() && m_is_start) {
    tsk = m_taskque.front();
    m_taskque.pop_front();
    assert(size - 1 == m_taskque.size());
  }
  return tsk;
}

