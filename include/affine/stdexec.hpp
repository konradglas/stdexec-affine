#pragma once

#include <affine/affine_task.hpp>

#include <stdexec/execution.hpp>

namespace affine::detail {

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
