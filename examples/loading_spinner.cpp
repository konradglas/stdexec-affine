// Interactive: type a key to "load" it on a worker while a spinner animates on main.
// A key typed during a load cancels it (latest wins). q quits. POSIX terminals only.
#include "../tests/test_main_queue.hpp"
#include "affine/task_scope.hpp"
#include <array>
#include <exec/single_thread_context.hpp>
#include <iostream>
#include <optional>
#include <poll.h>
#include <termios.h>
#include <thread>
#include <unistd.h>

using namespace affine;
using namespace std::chrono_literals;

struct Plugin {
    // a synchronouns call from your host.
    virtual void onEvent(char evt) = 0;
    virtual void onPaint() = 0;
};

struct Host {
    Host(TestMainQueue &queue) : queue(queue) {}
    TestMainQueue &queue;

    void addPlugin(Plugin *plugin) {
        this->plugin = plugin;
    }
    // One frame per iteration: input, paint, posted work. 'q' quits.
    void run() {
        RawTerminal raw;
        std::cout << "\033[2mtype a key to load it · q quits\033[0m\n";
        for (;;) {
            if (auto key = readKey()) {
                if (*key == 'q')
                    break;
                plugin->onEvent(*key);
            }
            plugin->onPaint();
            queue.drainPosts();
            std::this_thread::sleep_for(20ms);
        }
        std::cout << '\n';
    }

  private:
    // Keys arrive one by one, unechoed, without waiting for Enter. Restored on exit.
    struct RawTerminal {
        termios saved{};
        RawTerminal() {
            tcgetattr(STDIN_FILENO, &saved);
            termios raw = saved;
            raw.c_lflag &= ~(ICANON | ECHO);
            tcsetattr(STDIN_FILENO, TCSANOW, &raw);
        }
        ~RawTerminal() {
            tcsetattr(STDIN_FILENO, TCSANOW, &saved);
        }
    };

    // End of input counts as q.
    static std::optional<char> readKey() {
        pollfd fd{STDIN_FILENO, POLLIN, 0};
        if (poll(&fd, 1, 0) <= 0)
            return std::nullopt;
        char key;
        return read(STDIN_FILENO, &key, 1) == 1 ? key : 'q';
    }

  public:
    Plugin *plugin{};
};

struct Painter {
    // Repaints one line in place: \r goes back to its start, \033[K clears the rest.
    // Colors: \033[<n>m … \033[0m (2 dim, 1 bold, 31 red, 32 green, 36 cyan).
    void paint(bool loading, char current, std::optional<char> cancelled) {
        static constexpr std::array frames{"⠋", "⠙", "⠹", "⠸", "⠼",
                                           "⠴", "⠦", "⠧", "⠇", "⠏"};
        std::cout << '\r';
        if (loading) {
            std::cout << "\033[36m" << frames[paintIndex++ % frames.size()] << "\033[0m"
                      << " loading \033[1m" << current << "\033[0m";
            if (cancelled)
                std::cout << "  \033[2;31m" << *cancelled << " cancelled\033[0m";
        } else if (current != ' ') {
            std::cout << "\033[32m✓\033[0m loaded \033[1m" << current << "\033[0m";
        }
        std::cout << "\033[K" << std::flush;
    }
    int paintIndex = 0;
};

class MyPlugin : public Plugin {
  public:
    MyPlugin(TestMainQueue &queue)
        : _root([&queue](std::function<void()> callback) {
              queue.post(std::move(callback));
          }) {}
    // Join the tasks before the worker and the members they use are destroyed.
    ~MyPlugin() {
        _root.shutdown_and_wait();
    }
    void onEvent(char evt) override {
        // Latest wins: a load still running is cancelled.
        _cancelled = _handleLoad.completed() ? std::nullopt : std::optional{_requested};
        _handleLoad.close();
        _requested = evt;
        _handleLoad = spawn(handleLoad(evt), _root);
    };

    void onPaint() override {
        // Loading means: the current load task is still running. A stale task has
        // nothing to reset.
        bool loading = !_handleLoad.completed();
        painter.paint(loading, loading ? _requested : _toPrint, _cancelled);
    }

  private:
    stdexec::task<int> computeVeryDifficultThing() {
        auto stop = co_await stdexec::get_stop_token();
        int rounds = 50;
        while (rounds-- > 0) {
            if (stop.stop_requested()) {
                co_await stdexec::just_stopped();
            }
            // simulate expensive task
            std::this_thread::sleep_for(10ms);
        }
        co_return 42;
    }

    MainActorTask<void> handleLoad(char charToLoad) {
        co_await stdexec::starts_on(_worker.get_scheduler(),
                                    computeVeryDifficultThing());

        _toPrint = charToLoad;
    }

    affine::root_scope _root;
    affine::task_handle _handleLoad;
    exec::single_thread_context _worker;
    Painter painter;
    char _toPrint = ' ';
    char _requested = ' ';
    std::optional<char> _cancelled;
};

int main() {
    TestMainQueue queue;
    Host host(queue);
    MyPlugin myPlugin(queue);
    host.addPlugin(&myPlugin);
    host.run();
}
