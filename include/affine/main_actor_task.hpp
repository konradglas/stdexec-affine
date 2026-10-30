#pragma once

#include "affine/main_thread_scheduler.hpp"

#include <stdexec/execution.hpp>

namespace affine {

// A task whose body runs on the main loop and resumes there after every co_await.
// A stopped co_await unwinds the task instead of resuming it.
struct MainEnv {
    using start_scheduler_type = MainThreadLoop::Scheduler;
};

template<class T = void>
using MainActorTask = stdexec::task<T, MainEnv>;

} // namespace affine
