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
`include/affine/root_scope.hpp`, as a tree:

```cpp
root_scope scope(wakeup, onError);              // owns the MainThreadLoop
task_handle loading = spawn(load(), scope);     // starts on main
task_handle preset = spawn(loadPreset(), loading); // a child of loading
loading.close();                                // request stop for loading and preset; never waits
loading.shutdown_and_wait();                    // stop, join loading and preset, pump only their queues
scope.shutdown_and_wait();                      // stop all, join, drain main
```

- Every spawned task gets its own queue, a child domain of its parent's
  (`MainThreadLoop::make_domain`). Waiting on a node drains its own queue and its
  descendants', never a sibling's or an ancestor's, so joining the loading task in
  the middle of `load()` runs nothing unrelated.
- A task is counted in its own scope and in every ancestor's, and stop flows down
  from each node to its children: waiting on a node joins exactly its subtree, and
  `scope.shutdown_and_wait()` needs no handle waited first.
- `wait(true)` lets another thread take the subtree over while the owner is blocked.
- Handles can be copied and dropped freely; a parent keeps its children until its
  own wait joins them.
- An exception escaping a spawned task goes to `onError` (default: `std::cerr`).
- `spawn` may be called from any thread; once the parent has been joined a task is
  dropped unstarted and its handle is completed.

Continuations on main still complete inline when they are already on main; only
work arriving from other threads is queued.

Tasks see their node's plain `inplace_stop_token`. Do not nest stop-forwarding
scopes around a `MainActorTask` (`associate` into one scope, `spawn` into another):
that fuses the tokens, `stdexec::task` then keeps its own stop source in its frame,
and a stop that completes the task inline on main frees that source while its
`request_stop()` is still running. A `counting_scope` used directly is fine.

`task_scope_tests.cpp` checks these, including the two hand-off hazards fixed in
`main_thread_executor` (a waiting owner running continuations as they are queued,
and a joining thread retiring a task as soon as it completes).

## Build

Tested against the stdexec vendored in `stdexec-main-thread-playground` (`vendor/stdexec/REVISION`).

```sh
STDEXEC=/path/to/stdexec/include
clang++ -std=c++20 -I. -isystem $STDEXEC proto.cpp -o proto && ./proto
clang++ -std=c++20 -I. -isystem $STDEXEC nested.cpp -o nested && ./nested
clang++ -std=c++20 -I. -isystem $STDEXEC task_scope_tests.cpp -o task_scope_tests && ./task_scope_tests
```
