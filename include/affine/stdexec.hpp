#pragma once

#include <affine/affine_task.hpp>

#include <stdexec/execution.hpp>

namespace affine::detail {

template<class Promise>
struct environment_adapter<
    Promise, std::void_t<decltype(std::declval<Promise const&>().stop_token())>> {
    static auto make(Promise const& promise) noexcept {
        return stdexec::env{
            stdexec::prop{
                stdexec::get_stop_token,
                promise._state->_stop_source.get_token()}};
    }
};

template<class Sender, class Promise>
    requires stdexec::sender<Sender>
struct awaitable_adapter<Sender, Promise> {
    static constexpr bool customized = true;

    template<class S>
    static auto adapt(S&& sender, Promise& promise) {
        return stdexec::as_awaitable(
            static_cast<S&&>(sender), promise);
    }
};

} // namespace affine::detail
