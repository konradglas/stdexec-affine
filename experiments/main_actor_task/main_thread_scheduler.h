#pragma once

#include <stdexec/execution.hpp>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace playground {

// The hook posts the supplied callback to the owner thread. It must not call it
// inline, drop it during normal operation, or throw. Shutdown can leave posts pending.
using Wakeup = std::function<void(std::function<void()>)>;

class MainThreadLoop {
    struct Task {
        Task* next = nullptr;
        void (*execute)(Task*) noexcept = nullptr;
    };

    struct State : std::enable_shared_from_this<State> {
        inline static thread_local State* current = nullptr;
        struct CurrentGuard {
            explicit CurrentGuard(State& state) : previous(std::exchange(current, &state)) {}
            ~CurrentGuard() { current = previous; }
            State* previous;
        };
        explicit State(Wakeup wakeup)
        : wakeup(std::move(wakeup)), owner(std::this_thread::get_id()) {}

        ~State() { assert(head == nullptr); }

        void requireOwner() const {
            if (std::this_thread::get_id() != owner)
                throw std::logic_error("MainThreadLoop must be pumped on its owner thread");
        }

        void enqueue(Task* task) noexcept {
            bool needsWakeup;
            {
                std::lock_guard lock(mutex);
                assert(!finished && "Do not admit work after scope completion");
                needsWakeup = head == nullptr;
                if (tail)
                    tail->next = task;
                else
                    head = task;
                tail = task;
            }
            if (needsWakeup) {
                condition.notify_one();
                // Platform callbacks do not own the loop or pending operations.
                wakeup([weak = weak_from_this()] {
                    if (auto state = weak.lock())
                        state->drain();
                });
            }
        }

        std::size_t executeReady() {
            std::size_t count = 0;
            for (;;) {
                Task* batch;
                {
                    std::lock_guard lock(mutex);
                    batch = std::exchange(head, nullptr);
                    tail = nullptr;
                }
                if (!batch)
                    return count;
                while (batch) {
                    Task* task = batch;
                    batch = task->next;
                    // Completion is allowed to destroy the operation immediately.
                    task->execute(task);
                    ++count;
                }
            }
        }

        struct PumpGuard {
            explicit PumpGuard(State& state, bool allowNonOwner = false)
            : state(state), execution(state.execution), current(state) {
                if (!allowNonOwner)
                    state.requireOwner();
                if (state.pumping)
                    throw std::logic_error("MainThreadLoop pump is not reentrant");
                state.pumping = true;
            }
            ~PumpGuard() { state.pumping = false; }
            State& state;
            std::unique_lock<std::recursive_mutex> execution;
            CurrentGuard current;
        };

        std::size_t drain() {
            PumpGuard guard(*this);
            return executeReady();
        }

        void run() {
            PumpGuard guard(*this);
            for (;;) {
                executeReady();
                std::unique_lock lock(mutex);
                condition.wait(lock, [this] { return finished || head != nullptr; });
                if (finished && head == nullptr)
                    return;
            }
        }

        void finish() noexcept {
            {
                std::lock_guard lock(mutex);
                finished = true;
            }
            condition.notify_one();
        }

        Wakeup wakeup;
        const std::thread::id owner;
        // Serializes owner callbacks, inline scheduling and joining-thread takeover.
        std::recursive_mutex execution;
        std::mutex mutex;
        std::condition_variable condition;
        Task* head = nullptr;
        Task* tail = nullptr;
        bool finished = false;
        bool pumping = false; // Protected by execution.
    };

    struct WaitReceiver {
        using receiver_concept = stdexec::receiver_t;
        std::exception_ptr& error;
        bool& completed;
        bool& done;

        void set_value() noexcept {
            completed = true;
            done = true;
        }
        void set_stopped() noexcept { done = true; }
        template<class Error>
        void set_error(Error errorValue) noexcept {
            if constexpr (std::is_same_v<Error, std::exception_ptr>)
                error = std::move(errorValue);
            else
                error = std::make_exception_ptr(std::move(errorValue));
            done = true;
        }
        auto get_env() const noexcept { return stdexec::env{}; }
    };

public:
    class Scheduler {
    public:
        using scheduler_concept = stdexec::scheduler_t;

        struct Sender {
            using sender_concept = stdexec::sender_t;
            // Infallible: getting back onto the main thread never fails or stops.
            using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t()>;

            template<class Receiver>
            struct Operation : Task {
                Operation(std::shared_ptr<State> state, bool alwaysEnqueue, Receiver receiver)
                : state(std::move(state)), alwaysEnqueue(alwaysEnqueue), receiver(std::move(receiver)) {
                    this->execute = [](Task* task) noexcept {
                        auto& op = *static_cast<Operation*>(task);
                        stdexec::set_value(std::move(op.receiver));
                    };
                }
                Operation(const Operation&) = delete;
                Operation(Operation&&) = delete;

                void start() noexcept {
                    // Keep State alive even if a consumer completes and deletes us.
                    auto retainedState = state;
                    if (!alwaysEnqueue && (std::this_thread::get_id() == retainedState->owner
                                          || State::current == retainedState.get())) {
                        std::lock_guard execution(retainedState->execution);
                        State::CurrentGuard current(*retainedState);
                        {
                            std::lock_guard lock(retainedState->mutex);
                            assert(!retainedState->finished && "Do not admit work after scope completion");
                        }
                        this->execute(this);
                    } else {
                        retainedState->enqueue(this);
                    }
                }

                std::shared_ptr<State> state;
                bool alwaysEnqueue;
                Receiver receiver;
            };

            template<class Receiver>
            auto connect(Receiver receiver) const noexcept -> Operation<Receiver> {
                return {state, alwaysEnqueue, std::move(receiver)};
            }

            struct Environment {
                std::shared_ptr<State> state;
                bool alwaysEnqueue;
                auto query(stdexec::get_completion_scheduler_t<stdexec::set_value_t>) const noexcept -> Scheduler;
            };

            auto get_env() const noexcept -> Environment { return {state, alwaysEnqueue}; }
            std::shared_ptr<State> state;
            bool alwaysEnqueue;
        };

        auto schedule() const noexcept -> Sender { return {state, alwaysEnqueue}; }
        auto query(stdexec::get_forward_progress_guarantee_t) const noexcept {
            return stdexec::forward_progress_guarantee::parallel;
        }
        bool operator==(const Scheduler&) const = default;

    private:
        friend class MainThreadLoop;
        explicit Scheduler(std::shared_ptr<State> state, bool alwaysEnqueue)
        : state(std::move(state)), alwaysEnqueue(alwaysEnqueue) {}
        std::shared_ptr<State> state;
        bool alwaysEnqueue;
    };

    explicit MainThreadLoop(Wakeup wakeup) {
        if (!wakeup)
            throw std::invalid_argument("A main-thread wake-up hook is required");
        state = std::make_shared<State>(std::move(wakeup));
    }
    MainThreadLoop(const MainThreadLoop&) = delete;
    MainThreadLoop& operator=(const MainThreadLoop&) = delete;

    // Inline completion on the owner thread can reenter downstream user code.
    auto get_scheduler() const noexcept -> Scheduler { return Scheduler{state, false}; }
    auto get_enqueue_scheduler() const noexcept -> Scheduler { return Scheduler{state, true}; }
    // Separate continuation queue, same physical owner and platform dispatcher.
    // The returned sender's scheduler retains the domain state in its operation.
    auto make_domain() const -> std::unique_ptr<MainThreadLoop> {
        state->requireOwner();
        return std::make_unique<MainThreadLoop>(state->wakeup);
    }
    template<class Sender>
    auto bind(Sender&& sender) const {
        return stdexec::starts_on(get_scheduler(), std::forward<Sender>(sender));
    }
    std::size_t drain() { return state->drain(); }
    void run() { state->run(); }
    // Call only after all owned tasks complete; finish is not cancellation.
    void finish() noexcept { state->finish(); }

    // Reusable void-sender wait. Explicit takeover requires exclusive ownership
    // of user state: continuations may run on this thread instead of the owner.
    template<class Sender>
    bool sync_wait(Sender&& sender, bool allowNonOwnerDrain = false) {
        State::PumpGuard guard(*state, allowNonOwnerDrain);
        {
            std::lock_guard lock(state->mutex);
            if (state->finished)
                throw std::logic_error("MainThreadLoop has already finished");
        }
        std::exception_ptr error;
        bool completed = false;
        bool done = false;
        auto operation = stdexec::connect(
            stdexec::continues_on(
                stdexec::starts_on(get_scheduler(), std::forward<Sender>(sender)),
                get_enqueue_scheduler()),
            WaitReceiver{error, completed, done});
        stdexec::start(operation);
        while (!done) {
            state->executeReady();
            if (done)
                break;
            std::unique_lock lock(state->mutex);
            state->condition.wait(lock, [&] { return state->head != nullptr; });
        }
        if (error)
            std::rethrow_exception(error);
        return completed;
    }

private:
    std::shared_ptr<State> state;
};

inline auto MainThreadLoop::Scheduler::Sender::Environment::query(
    stdexec::get_completion_scheduler_t<stdexec::set_value_t>) const noexcept -> Scheduler {
    return Scheduler{state, alwaysEnqueue};
}

static_assert(stdexec::scheduler<MainThreadLoop::Scheduler>);

} // namespace playground
