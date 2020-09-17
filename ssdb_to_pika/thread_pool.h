#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <deque>
#include <thread>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <memory> 

class ThreadPool{
  public:
    typedef std::function<void()> Task;

    ThreadPool(int initial_size = 15);

    ~ThreadPool();

    void Stop();

    void AddTask(const Task& task);

    ThreadPool(const ThreadPool& rhs) = delete;

    const ThreadPool& operator = (const ThreadPool& rhs) = delete;

  private:
    bool is_started() { return is_started_;}

    void Start();

    void ThreadLoop();

    Task Take();

    typedef std::vector<std::unique_ptr<std::thread>> Worker;
    typedef std::deque<Task> TaskQue;
    int worker_size_;
    Worker workthreads_;
    TaskQue taskque_;
    std::mutex mutex_;
    std::condition_variable cond_;    
    bool is_started_; 
};

#endif
