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

This project is based on the execution model used by stdexec and is designed
to interoperate with sender/receiver executors, but it does not currently
depend on a particular stdexec checkout.

## Status

Early development. The executor contract and shutdown API are expected to
evolve before the first stable release.
