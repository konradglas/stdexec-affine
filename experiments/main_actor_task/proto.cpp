#include "main_thread_scheduler.h"
#include "test_main_queue.h"
#include <exec/single_thread_context.hpp>
#include <iostream>
#include <thread>

// The whole "framework": a task whose start scheduler type is the main loop.
struct MainEnv {
  using start_scheduler_type = playground::MainThreadLoop::Scheduler;
};
template <class T> using MainActorTask = stdexec::task<T, MainEnv>;

MainActorTask<void> loadPreset(std::thread::id mainThread, auto worker, bool &done) {
  std::cout << "starts on main:  " << (std::this_thread::get_id() == mainThread) << '\n';
  int value = co_await (stdexec::schedule(worker) | stdexec::then([mainThread] {
                          std::cout << "work on worker:  " << (std::this_thread::get_id() != mainThread) << '\n';
                          return 42;
                        }));
  std::cout << "back on main:    " << (std::this_thread::get_id() == mainThread) << ", value = " << value << '\n';
  done = true;
}

int main() {
  playground::TestMainQueue platformQueue;
  playground::MainThreadLoop loop([&](auto callback) { platformQueue.post(std::move(callback)); });
  exec::single_thread_context worker;
  bool done = false;
  loop.sync_wait(loadPreset(std::this_thread::get_id(), worker.get_scheduler(), done));
  loop.finish();
  platformQueue.drainPosts();
  return done ? 0 : 1;
}
