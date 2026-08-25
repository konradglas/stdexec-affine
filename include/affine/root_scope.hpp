#pragma once

#include <affine/affine_task.hpp>

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace affine {

namespace detail {

struct scoped_operation {
    virtual ~scoped_operation() = default;
    virtual void start() = 0;
    virtual void close() noexcept = 0;
    virtual void drain() = 0;
    [[nodiscard]] virtual bool completed() const noexcept = 0;
};

template<class Executor>
class scoped_task_operation final : public scoped_operation {
public:
    explicit scoped_task_operation(affine_task<void, Executor> task)
        : _task(std::move(task)) {}

    void start() override { _task.start(); }

    void close() noexcept override {
        _task.request_stop();
        if constexpr (requires(Executor& executor) { executor.close(); }) {
            try {
                _task.executor().close();
            } catch (...) {
                std::terminate();
            }
        }
    }

    void drain() override {
        const auto state = _task.state();
        if (!state)
            return;

        auto& executor = _task.executor();
        if (!state->is_completed()) {
            if constexpr (drainable_executor<Executor>) {
                executor.run_until([&] { return state->is_completed(); });
            } else {
                throw std::logic_error(
                    "root_scope requires a drainable executor for active tasks");
            }
        }

        if constexpr (drainable_executor<Executor>)
            executor.drain();

        if (state->_error)
            std::rethrow_exception(state->_error);
    }

    [[nodiscard]] bool completed() const noexcept override {
        const auto state = _task.state();
        return !state || state->is_completed();
    }

private:
    affine_task<void, Executor> _task;
};

struct root_scope_state {
    ~root_scope_state() {
        std::lock_guard lock(_mutex);
        if (!_operations.empty())
            std::terminate();
    }

    void add(const std::shared_ptr<scoped_operation>& operation) {
        std::lock_guard lock(_mutex);
        if (_closed)
            throw std::logic_error("cannot spawn into a closed root_scope");
        _operations.push_back(operation);
    }

    void remove(const std::shared_ptr<scoped_operation>& operation) noexcept {
        std::lock_guard lock(_mutex);
        std::erase(_operations, operation);
    }

    void close() noexcept {
        std::vector<std::shared_ptr<scoped_operation>> operations;
        {
            std::lock_guard lock(_mutex);
            if (_closed)
                return;
            _closed = true;
            operations = _operations;
        }
        for (const auto& operation : operations)
            operation->close();
    }

    void drain() {
        std::vector<std::shared_ptr<scoped_operation>> operations;
        {
            std::lock_guard lock(_mutex);
            if (!_closed)
                throw std::logic_error("root_scope must be closed before it is drained");
            operations = _operations;
        }

        std::exception_ptr error;
        for (const auto& operation : operations) {
            try {
                operation->drain();
            } catch (...) {
                if (!error)
                    error = std::current_exception();
            }
        }

        {
            std::lock_guard lock(_mutex);
            std::erase_if(_operations, [](const auto& operation) {
                return operation->completed();
            });
        }

        if (error)
            std::rethrow_exception(error);
    }

    mutable std::mutex _mutex;
    bool _closed = false;
    std::vector<std::shared_ptr<scoped_operation>> _operations;
};

} // namespace detail

class root_scope {
public:
    root_scope() : _state(std::make_shared<detail::root_scope_state>()) {}

    void close() const noexcept { _state->close(); }
    void drain() const { _state->drain(); }

private:
    template<class Executor>
    friend void spawn(affine_task<void, Executor>, const root_scope&);

    std::shared_ptr<detail::root_scope_state> _state;
};

inline root_scope create_root_scope() { return {}; }

template<class Executor>
void spawn(affine_task<void, Executor> task, const root_scope& scope) {
    if (!task.valid())
        throw std::invalid_argument("spawn requires a valid affine_task");

    auto operation =
        std::make_shared<detail::scoped_task_operation<Executor>>(std::move(task));
    scope._state->add(operation);
    try {
        operation->start();
    } catch (...) {
        scope._state->remove(operation);
        throw;
    }
}

} // namespace affine
