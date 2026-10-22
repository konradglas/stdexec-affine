# MainActorTask on top of stdexec::task

Experiment: can the main-thread affinity this library provides with
`affine_task` come from `stdexec::task` (the C++26 `std::execution::task`
implementation) plus a one-line alias, instead of a custom task type?

```cpp
struct MainEnv {
  using start_scheduler_type = playground::MainThreadLoop::Scheduler;
};
template <class T> using MainActorTask = stdexec::task<T, MainEnv>;
```

The scheduler here is the `MainThreadLoop` from the
`stdexec-main-thread-playground`, not this library's `main_thread_executor`.

## Results

- `proto.cpp`: starts on main, `co_await`s worker work, comes back to main
  by itself. No explicit hop back to main is needed.
- Starting a `MainActorTask` without a main scheduler in the environment
  (e.g. plain `stdexec::sync_wait`) does not compile.
- `nested.cpp`: a plain `stdexec::task` cannot `co_await` a `MainActorTask`
  directly (compile error: its scheduler is type-erased). It works with
  `co_await stdexec::starts_on(loop.get_scheduler(), mainTask())`.
  Build with `-DDIRECT` to see the error.

## The catch

`stdexec::task` only resumes on schedulers whose schedule sender is
infallible (`set_value_t()` only). The `main_thread_scheduler.h` in this
folder is a copy of the playground one with `set_stopped` removed; the
original also completes with `set_stopped`, which `stdexec::task` rejects
with `_THE_SCHEDULER_IN_THE_CURRENT_EXECUTION_ENVIRONMENT_IS_NOT_INFALLIBLE_`.

Cancellation still works: awaited operations complete with stopped as
usual; only the hop back to main always goes through.

## Owning tasks: `root_scope` and `task_handle`

`task_scope.h` gives `MainActorTask` (`main_actor_task.h`) the ownership model of
`include/affine/root_scope.hpp`:

```cpp
root_scope scope(wakeup, onError);           // owns the MainThreadLoop
task_handle loading = spawn(load(), scope);  // starts on main
loading.close();                             // request stop; never waits
loading.completed();
scope.shutdown_and_wait();                   // the only wait: stop all, join, drain main
```

- `close()` only requests stop. A stopped `co_await` unwinds the task; RAII
  cleanup runs on main.
- `shutdown_and_wait()` joins on the owner thread by draining the loop's own
  queue, so it never needs the host message loop.
- An exception escaping a spawned task goes to `onError` (default: `std::cerr`).
- `spawn` may be called from any thread; after shutdown a task is dropped
  unstarted and its handle is completed.

`task_scope_tests.cpp` checks these.

## Build

Tested against the stdexec pinned in `ADC_Coroutine_demo/third_party/stdexec`.

```sh
STDEXEC=/path/to/stdexec/include
clang++ -std=c++20 -I. -isystem $STDEXEC proto.cpp -o proto && ./proto
clang++ -std=c++20 -I. -isystem $STDEXEC nested.cpp -o nested && ./nested
clang++ -std=c++20 -I. -isystem $STDEXEC task_scope_tests.cpp -o task_scope_tests && ./task_scope_tests
```
