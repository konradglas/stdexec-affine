# stdexec-affine: guidance for agents

Header-only C++20 library on stdexec: `MainActorTask` (a `stdexec::task` that starts on and resumes on the main
thread) and an ownership tree of task scopes. Read `README.md` for the model; this file is about using and changing it.

## The pieces

- `include/affine/main_thread_scheduler.hpp`: `MainThreadLoop`, the main thread's queue. The host supplies a `Wakeup`
  that posts a callback to its real main thread (`dispatch_async`, a UI dispatcher). It must post, never call inline.
- `include/affine/main_actor_task.hpp`: `MainActorTask<T>`. Its body runs on main and returns to main after every
  `co_await`, with no explicit hop.
- `include/affine/task_scope.hpp`: the ownership tree.
  - `root_scope`: one per app, owns the loop.
  - `child_scope`: a node without a task, for an owner that spawns tasks over its lifetime (a document, a view).
    Destroying it stops and joins exactly its own tasks.
  - `task_handle`: one spawned task and its children; `close()` requests stop, `wait()` joins.
  - `spawn(task, parent)`: starts a task under a scope or a handle, from any thread.

## Writing tasks

- One owner per piece of async work. An object that starts work holds a `child_scope` (or `task_handle`s) and joins it
  before the state the tasks use is destroyed, normally first thing in its destructor.
- Keep the tree flat: spawn into the root or the owner's `child_scope`, not under another task. Keep a `task_handle`
  only for work that is closed or waited on individually.
- Never immediately invoke a coroutine lambda that captures state: the closure dies while the coroutine still uses it.
  Write a named coroutine function (a member is fine) and pass what it needs as parameters, which live in the frame.
- A task spawned on the main thread runs inline up to its first hop, and the main scheduler completes inline on main.
  When the rest must not run inside the caller (it calls UI code, or the caller holds locks), first
  `co_await stdexec::schedule(scope.enqueue_scheduler())`, which always queues.
- Leave the main thread with `co_await (stdexec::schedule(pool.get_scheduler()) | stdexec::then(...))`. Pass immutable
  input into the work and return results; the work must not touch main-thread state.
- Run a whole coroutine on a pool with `co_await stdexec::starts_on(pool.get_scheduler(), work())`, where `work()` is a
  plain `stdexec::task`: it resumes on the pool after its own `co_await`s, and the caller resumes on main afterwards.
- Stop tokens are `stdexec::inplace_stop_token`, read with `co_await stdexec::read_env(stdexec::get_stop_token)`. They
  are pointers to a stop source in the task's node, with no allocation: never let one outlive the task (passing it into
  work the task awaits is fine). Do not convert to `std::stop_token` unless an API demands it.
- Never put a different stop-token type into a task's environment, neither by `write_env` nor through a scope that
  forwards stop to its own source. A `stdexec::task` that receives a token of another type keeps its own stop source
  in its frame. A stop can complete such a task inline on main, for example as a waiter of `split`. Its parent then
  frees the frame while that stop source is still running its callbacks. That is a use-after-free in
  `inplace_stop_source::request_stop()`. The split test in `tests/task_scope_test.cpp` shows the safe case.
- Cancellation is cooperative. Long CPU work takes the token and checks it at bounded intervals (per line, per item,
  per block), returning early without publishing partial results (e.g. do not fill a shared cache from a cancelled run).
  Code that cannot stop is a gap to fix, not a reason to skip cancellation.

## Obsolete work: cancel it, do not count generations

A generation counter (stamp each request, compare the stamp when the result arrives) is never needed when the cause
(the request becoming obsolete) and the effect (applying the result) happen on the same thread, or are serialized by
one lock. Then requesting stop before the effect is enough: the effect checks the request's stop token right before it
applies, and a stopped request never applies. A counter only stands in for that missing serialization; fix the
serialization instead.

With this library, the owner closes the task once it is obsolete, and the task checks its stop token before it applies
its result:

```cpp
// Cause, under the lock that guards the state (or on main, where the effect also runs):
if (auto previous = loads.extract(track))
    previous.mapped().close();                       // the old request is obsolete: stop it
loads[track] = affine::spawn(load(track, request), scope);

// Effect, after the background work, back on main:
auto stop = co_await stdexec::read_env(stdexec::get_stop_token);
auto result = co_await (stdexec::schedule(pool.get_scheduler()) | stdexec::then([&, stop] { return work(stop); }));
std::lock_guard lock(mutex);                           // the same lock every close runs under
if (stop.stop_requested())
    co_return;                                         // obsolete: never applied
apply(std::move(result));
```

What this needs:

- Every way a request becomes obsolete requests stop for it: a newer request for the same thing, a reset of the state
  it targets, the owner going away (its scope's destructor), and any path that does the work some other way (e.g. a
  synchronous export taking over pending loads).
- The stop request and the check-then-apply are serialized. On the main thread they already are; if the stop can come
  from another thread, hold the same lock around it and around the check and the apply.

The same holds for work that is not a task, e.g. a callback API without cancellation: give each request its own stop
source (or hold it in a task that awaits the callback), request stop when the request becomes obsolete, and have the
callback check it before applying, on the same thread or under the same lock.

Track what remains explicitly instead of with stamps: model what a thing is doing as a `std::variant` of its states
(e.g. `Installed{id} | Loading{id, request}`), and assert on transitions that cannot happen (installing into something
that is not `Loading`).

Not to be confused with a version check against state you do not own (a persisted or remote record that can change
under you): comparing against that state's version is about the data, not about obsolete work, and is still needed.

## Cleanup: undo only what this task acquired

A scope guard in a task (a `ScopeExit`, a destructor) runs when the task ends, also when it is stopped while a newer
task already runs. It must undo only what this task did. Resetting shared state (`setLoading(false)`) undoes what every
task did: an obsolete task that unwinds late turns off the newer task's spinner.

- "Something is happening" state (a spinner, a busy cursor): let each task acquire its own share, counted by the owner
  and shown while the count is above zero. The share's destructor gives back only this task's part:

  ```cpp
  auto busy = app.beginLoading();   // ++count; ~Busy does --count, also when stopped
  ```

  Or derive it from the owner instead of storing it: `isLoading() = !loading.completed()`.
- "Which result counts" state (the installed preset): last writer wins, guarded by the stop token as in the section
  above. A stopped task never reaches the assignment.

## Changing the library

- Keep `child_scope`, `root_scope` and `task_handle` consistent: joining a node joins exactly its subtree and pumps only
  its queues; a node an ancestor has joined waits at once; tasks spawned into a joined node never start.
- Every change gets a check in `tests/task_scope_test.cpp` (the `check(...)` style there) and must pass:
  - plain: `clang++ -std=c++20 -Iinclude -Itests -isystem $STDEXEC tests/task_scope_test.cpp && ./a.out`
  - `-fsanitize=address,undefined` and, separately, `-fsanitize=thread`
  - CI (`.github/workflows/toolchains.yml`): Apple clang, MSVC and the Android NDK, against the pinned stdexec revision
    in `CMakeLists.txt`.
- The old `affine_task` / `main_thread_executor` library lives on the `deprecated/affine-task` branch; do not
  reintroduce it on `main`.
