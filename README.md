# stdexec-affine

Main-thread affine coroutines on `stdexec::task` (the C++26 `std::execution::task`
implementation): a task body runs on the main thread, `co_await`s work anywhere,
and resumes on the main thread after every `co_await` by itself.

```cpp
struct MainEnv {
  using start_scheduler_type = affine::MainThreadLoop::Scheduler;
};
template <class T = void> using MainActorTask = stdexec::task<T, MainEnv>;
```

`include/affine/main_actor_task.hpp` defines `MainActorTask`,
`include/affine/main_thread_scheduler.hpp` the `MainThreadLoop` it runs on, and
`include/affine/task_scope.hpp` the ownership model below.

The earlier `affine_task` / `main_thread_executor` library is no longer developed;
it is kept on the `deprecated/affine-task` branch (tag `affine-task-final`).

## Behaviour

- `examples/round_trip.cpp`: starts on main, `co_await`s worker work, comes back to
  main by itself. No explicit hop back to main is needed.
- Starting a `MainActorTask` without a main scheduler in the environment
  (e.g. plain `stdexec::sync_wait`) does not compile.
- `examples/nested.cpp`: a plain `stdexec::task` cannot `co_await` a `MainActorTask`
  directly (compile error: its scheduler is type-erased). It works with
  `co_await stdexec::starts_on(loop.get_scheduler(), mainTask())`.
  Build with `-DDIRECT` to see the error.

## The catch

`stdexec::task` only resumes on schedulers whose schedule sender is
infallible (`set_value_t()` only). The `MainThreadLoop` scheduler therefore never
completes with `set_stopped`; `stdexec::task` rejects a scheduler that does with
`_THE_SCHEDULER_IN_THE_CURRENT_EXECUTION_ENVIRONMENT_IS_NOT_INFALLIBLE_`.

Cancellation still works: awaited operations complete with stopped as
usual; only the hop back to main always goes through.

## Owning tasks: `root_scope`, `child_scope` and `task_handle`

`task_scope.hpp` gives `MainActorTask` an ownership model, as a tree:

```cpp
root_scope scope(wakeup, onError);              // owns the MainThreadLoop
task_handle loading = spawn(load(), scope);     // starts on main
task_handle preset = spawn(loadPreset(), loading); // a child of loading
loading.close();                                // request stop for loading and preset; never waits
loading.shutdown_and_wait();                    // stop, join loading and preset, pump only their queues
scope.shutdown_and_wait();                      // stop all, join, drain main
```

An owner that spawns tasks over its lifetime, rather than being a task itself, holds a
`child_scope`: a node without a task, under the root or under a task.

```cpp
class Document {
  affine::child_scope loads;                    // e.g. child_scope loads{root}
  void instrumentChanged() { spawn(loadInstrument(), loads); }
  // ~child_scope stops and joins this document's loads only
};
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

`tests/task_scope_test.cpp` checks these, including two hand-off hazards: a waiting
owner running continuations as they are queued, and a joining thread retiring a task
as soon as it completes.

## Build

Header-only; needs C++20 and stdexec. Tested against NVIDIA/stdexec
`fb944c333ae00d11cc0c56e1fb9b2bce04b5e162`.

```sh
cmake -B build -DSTDEXEC_AFFINE_STDEXEC_SOURCE_DIR=/path/to/stdexec
cmake --build build && ctest --test-dir build
```

Or directly:

```sh
STDEXEC=/path/to/stdexec/include
clang++ -std=c++20 -Iinclude -Itests -isystem $STDEXEC tests/task_scope_test.cpp -o task_scope_test && ./task_scope_test
clang++ -std=c++20 -Iinclude -Itests -isystem $STDEXEC examples/round_trip.cpp -o round_trip && ./round_trip
```

A parent project that already has a `STDEXEC::stdexec` target can `add_subdirectory`
this repository and link `stdexec_affine::affine`.
