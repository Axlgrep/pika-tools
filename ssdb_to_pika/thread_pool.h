#ifndef THREADPOOL_H
#define THREADPOOL_H

#include<vector>
#include<deque>
#include<thread>
#include<functional>
#include<mutex>
#include<condition_variable>

class ThreadPool{
  public:
    typedef std::function<void()> task_t;
    ThreadPool(int initial_size = 15);
    ~ThreadPool();
    void Stop();
    void AddTask(const task_t& task);
    ThreadPool(const ThreadPool& rhs) = delete;
    const ThreadPool& operator=(const ThreadPool& rhs) = delete;
  private:
    bool IsStarted() { return m_is_start;}
    void Start();
    void ThreadLoop();
    task_t Take();
    typedef std::vector<std::thread*> Worker;
    typedef std::deque<task_t> TaskQue;
    int m_size;
    Worker m_workthreads;
    TaskQue m_taskque;
    std::mutex m_mutex;
    std::condition_variable m_cond;    
    bool m_is_start; 
};

#endif
