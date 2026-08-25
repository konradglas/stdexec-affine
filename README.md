# stdexec-affine

Executor-affine C++20 coroutine tasks for stdexec-style applications.

The first version is intentionally small. Its central contract is:

* a task and its nested tasks have an executor affinity;
* resumption is scheduled onto that executor;
* an executor may cooperatively drive its queue while a caller synchronously
  waits on the executor thread;
* task ownership and completion remain explicit during shutdown.

The generic type is `affine::affine_task<T, Executor>`. A main-thread integration
can expose `affine::main_actor_task<T, MainExecutor>` without baking a GUI or
host API into this library.

The core header is standalone:

```cpp
#include <affine/affine_task.hpp>
```

For stdexec sender adaptation, include the optional integration header instead:

```cpp
#include <affine/stdexec.hpp>
```

The executor concepts are available independently through
`<affine/executor.hpp>`. The host-facing main-thread adapter is opt-in through
`<affine/main_thread_executor.hpp>` and takes host `dispatch` and optional
event-pump callbacks. Executors that own a ready queue may additionally model
`affine::drainable_executor`; `main_thread_executor::drain()` must be called on
its owner thread before task frames are destroyed when claimed callbacks need
to be completed.

Detached roots are owned explicitly through `<affine/root_scope.hpp>`:

```cpp
auto root_scope = affine::create_root_scope();
affine::spawn(root_task(executor), root_scope);

// During teardown, on the executor's owner thread:
root_scope.close();
root_scope.drain();
```

`close()` requests cooperative stop and closes executors that provide a
`close()` operation. In-flight operations may still publish their final
continuation into the executor's local ready queue. `drain()` keeps every root
frame alive and drives that queue until all structurally awaited work has
finished; only then are the frames destroyed.
The last `root_scope` handle must not be destroyed while it still owns tasks;
call `close()` and `drain()` during teardown.

Tasks support cooperative cancellation through their context:

```cpp
affine::affine_task<void, Executor> work(Executor executor) {
    auto token = co_await affine::this_task::stop_token;
    if (token.stop_requested())
        co_return;
    // cancellable work...
}
```

Call `task.request_stop()` to request cancellation. Awaitables are not forced
to stop and must opt in by observing the token. If an operation completes with
stop, `sync_wait()` throws `affine::operation_cancelled`; use stdexec's
`stopped_as_optional` when cancellation should become a value such as
`std::nullopt` instead.

This project is based on the execution model used by stdexec and is designed
to interoperate with sender/receiver executors, but it does not currently
depend on a particular stdexec checkout.

## Status

Early development. The executor contract and shutdown API are expected to
evolve before the first stable release.
