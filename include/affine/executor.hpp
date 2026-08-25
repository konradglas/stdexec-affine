#pragma once

#include <concepts>
#include <coroutine>

namespace affine {

template<class E>
concept executor = requires(E& value, std::coroutine_handle<> handle) {
    { value.is_current_thread() } -> std::same_as<bool>;
    { value.schedule(handle) } -> std::same_as<bool>;
};

template<class E>
concept sync_executor = executor<E> && requires(E& value) {
    value.run_until([] { return true; });
};

template<class E>
concept drainable_executor = executor<E> && requires(E& value) {
    value.drain();
};

} // namespace affine
