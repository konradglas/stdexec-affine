#pragma once

#include <concepts>
#include <coroutine>

namespace affine {

// `schedule` must not touch the executor object once the handle may run:
// the caller's copy can live in the frame that the handle resumes and destroys.
template<class E>
concept executor = requires(E& value, std::coroutine_handle<> handle) {
    { value.is_current_thread() } -> std::same_as<bool>;
    { value.schedule(handle) } -> std::same_as<bool>;
};

template<class E>
concept drainable_executor = executor<E> && requires(E& value) {
    value.run_until([] { return true; });
    value.drain();
};

} // namespace affine
