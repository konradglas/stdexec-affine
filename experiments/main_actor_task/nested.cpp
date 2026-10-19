#include "main_thread_scheduler.h"
#include "test_main_queue.h"
#include <exec/single_thread_context.hpp>
#include <iostream>
#include <thread>

struct MainEnv {
  using start_scheduler_type = playground::MainThreadLoop::Scheduler;
};
template <class T> using MainActorTask = stdexec::task<T, MainEnv>;

MainActorTask<int> updateUi(std::thread::id mainThread) {
  std::cout << "ui on main:      " << (std::this_thread::get_id() == mainThread) << '\n';
  co_return 7;
}

// An ordinary task: it does not know about the main loop.
stdexec::task<void> generic(std::thread::id mainThread, playground::MainThreadLoop &loop, bool &done) {
#ifdef DIRECT
  int v = co_await updateUi(mainThread);
#else
  int v = co_await stdexec::starts_on(loop.get_scheduler(), updateUi(mainThread));
#endif
  std::cout << "generic got " << v << '\n';
  done = true;
}

int main() {
  playground::TestMainQueue platformQueue;
  playground::MainThreadLoop loop([&](auto callback) { platformQueue.post(std::move(callback)); });
  bool done = false;
  loop.sync_wait(generic(std::this_thread::get_id(), loop, done));
  loop.finish();
  platformQueue.drainPosts();
  return done ? 0 : 1;
}
