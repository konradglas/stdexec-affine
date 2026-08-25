# stdexec-affine

Drain executor-affine C++20 coroutines from the executor's own thread without
deadlocking.

The central abstraction is a **drainable executor**. A thread may synchronously
wait for work that resumes on that same thread because it does not block; it
cooperatively executes the executor's queued continuations until the task is
complete.

```text
executor thread starts an affine task
    -> task awaits background work
    -> background work queues the continuation on the executor
    -> executor thread drains and resumes that continuation itself
    -> task completes
```

The primary use case is a DLL or dylib driven by a host-owned main/message
thread. The library has no application `main()` and no direct control over the
host's message queue, yet initialization, callbacks, and teardown all happen
on that thread. Audio plug-ins are the clearest example: teardown must finish
synchronously before the host may unload the plug-in, even when outstanding
coroutines need to return to the message thread first.

## Execution model

An `affine::executor` provides sticky execution: task bodies and continuations
return to that executor. A task may still await work on a thread pool or any
other execution context.

An `affine::drainable_executor` additionally provides both operations needed
for same-thread synchronous waiting:

```cpp
executor.drain();
executor.run_until(predicate);
```

`drain()` executes work already claimed for local execution. `run_until()`
keeps draining—and, during normal operation, may cooperatively pump a host
queue—until the predicate is true.

The generic task type is:

```cpp
#include <affine/affine_task.hpp>

affine::affine_task<T, Executor>
```

The optional stdexec integration adapts senders and `exec::task` while
preserving executor affinity:

```cpp
#include <affine/stdexec.hpp>
```

## Synchronous teardown

Detached roots are owned explicitly by a root scope:

```cpp
#include <affine/root_scope.hpp>

auto root_scope = affine::create_root_scope();
affine::spawn(root_task(executor), root_scope);

// On the executor's owner thread during teardown:
root_scope.close();
root_scope.drain();
```

`close()` requests cooperative stop and closes executors that expose a
`close()` operation. The supplied `main_thread_executor` then claims callbacks
already submitted to the host queue. Completions arriving after closure are
placed directly into its local ready queue.

`drain()` keeps every structurally owned coroutine frame alive and drives the
executor until all roots have completed. Only then are the frames destroyed.
This is why waiting on the executor thread does not deadlock.

After shutdown begins, an in-flight operation must eventually complete without
requiring another external host message-loop event. Cancellation is
cooperative and is not mandatory for every awaitable.

The last `root_scope` handle must not be destroyed while it still owns tasks;
call `close()` and `drain()` during teardown.

## Host-owned main threads

`<affine/main_thread_executor.hpp>` adapts a host dispatch queue and optional
event pump. During normal operation it dispatches continuations through the
host. After `close()`, it stops adding host callbacks and drains locally on its
owner thread.

Already-submitted host callbacks may still run later as claimed no-ops. A host
that unloads plug-in code must therefore place its own unload barrier after
plug-in teardown.

## Cancellation

Tasks expose cooperative cancellation through their context:

```cpp
affine::affine_task<void, Executor> work(Executor executor) {
    auto token = co_await affine::this_task::stop_token;
    if (token.stop_requested())
        co_return;

    // cancellable work...
}
```

Call `task.request_stop()` to request cancellation. Awaitables opt in by
observing the token. If an operation completes with stop, `sync_wait()` throws
`affine::operation_cancelled`; use stdexec's `stopped_as_optional` when stop
should instead become a value such as `std::nullopt`.

## Status

Early development. The executor contract and shutdown API may evolve before
the first stable release.
