// Interactive: type a letter and a worker looks up a random word starting with it,
// while a spinner animates on main. A key typed during a lookup cancels it (latest wins).
// Esc quits. POSIX terminals only. Run it from the repository root: it reads
// examples/words.txt (from Webster's 2nd International, public domain).
#include "../tests/test_main_queue.hpp"
#include "affine/task_scope.hpp"
#include <array>
#include <cctype>
#include <exec/single_thread_context.hpp>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <vector>
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
    // One frame per iteration: input, paint, posted work. Esc quits.
    void run() {
        RawTerminal raw;
        std::cout << "\033[2mtype a letter to find a word · esc quits\033[0m\n";
        for (;;) {
            if (auto key = readKey()) {
                if (*key == '\033')
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

    // End of input counts as Esc.
    static std::optional<char> readKey() {
        pollfd fd{STDIN_FILENO, POLLIN, 0};
        if (poll(&fd, 1, 0) <= 0)
            return std::nullopt;
        char key;
        return read(STDIN_FILENO, &key, 1) == 1 ? key : '\033';
    }

  public:
    Plugin *plugin{};
};

struct Painter {
    // Repaints one line in place: \r goes back to its start, \033[K clears the rest.
    // Colors: \033[<n>m … \033[0m (2 dim, 1 bold, 31 red, 32 green, 36 cyan).
    void paint(bool loading, char key, const std::string &word, std::optional<char> cancelled) {
        static constexpr std::array frames{"⠋", "⠙", "⠹", "⠸", "⠼",
                                           "⠴", "⠦", "⠧", "⠇", "⠏"};
        std::cout << '\r';
        if (loading) {
            std::cout << "\033[36m" << frames[paintIndex++ % frames.size()] << "\033[0m"
                      << " looking up \033[1m" << key << "\033[0m";
            if (cancelled)
                std::cout << "  \033[2;31m" << *cancelled << " cancelled\033[0m";
        } else if (!word.empty()) {
            std::cout << "\033[32m✓\033[0m \033[1m" << key << "\033[0m → " << word;
        } else if (key != ' ') {
            std::cout << "\033[2mno word for " << key << "\033[0m";
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
        _root.close_and_wait();
    }
    void onEvent(char evt) override {
        // Latest wins: a load still running is cancelled.
        _cancelled = _handleLoad.completed() ? std::nullopt : std::optional{_requested};
        _handleLoad.close();
        _requested = static_cast<char>(std::tolower(evt));
        _handleLoad = spawn(handleLoad(_requested), _root);
    };

    void onPaint() override {
        // Loading means: the current load task is still running. A stale task has
        // nothing to reset.
        bool loading = !_handleLoad.completed();
        painter.paint(loading, _requested, loading ? std::string{} : _loaded, _cancelled);
    }

  private:
    // Runs on the worker: reads the whole list and picks a random word starting with
    // the letter. Checks the stop token on every line, so a newer key stops it early.
    static stdexec::task<std::string> findWord(char letter) {
        auto stop = co_await stdexec::get_stop_token();
        std::ifstream file("examples/words.txt");
        std::vector<std::string> matches;
        std::string line;
        for (int n = 0; std::getline(file, line); ++n) {
            if (stop.stop_requested())
                co_await stdexec::just_stopped();
            if (!line.empty() && line[0] == letter)
                matches.push_back(line);
            if (n % 100 == 0)
                std::this_thread::sleep_for(4ms); // reading is fast: slow it down to watch it
        }
        if (matches.empty())
            co_return "";
        std::mt19937 random{std::random_device{}()};
        co_return matches[std::uniform_int_distribution<size_t>(0, matches.size() - 1)(random)];
    }

    MainActorTask<void> handleLoad(char letter) {
        auto word = co_await stdexec::starts_on(_worker.get_scheduler(), findWord(letter));
        _loaded = word;
    }

    affine::root_scope _root;
    affine::task_handle _handleLoad;
    exec::single_thread_context _worker;
    Painter painter;
    char _requested = ' '; // what the user asked for: the source of truth
    std::string _loaded;   // derived from _requested by a finished lookup; lags behind it
    std::optional<char> _cancelled;
};

int main() {
    if (!std::ifstream("examples/words.txt")) {
        std::cerr << "examples/words.txt not found: run this from the repository root\n";
        return 1;
    }
    TestMainQueue queue;
    Host host(queue);
    MyPlugin myPlugin(queue);
    host.addPlugin(&myPlugin);
    host.run();
}
