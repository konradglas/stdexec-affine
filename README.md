# stdexec-affine

Coroutines that always come back to the main thread.

Write async code the way you'd write the blocking version: the body runs on main,
the heavy part runs on a worker, and after every `co_await` you're back on main.
No hop back, no locks, and RAII works again.

```cpp
affine::MainActorTask<void> loadPreset(Preset preset) {
    auto busy = app.beginLoading();                  // spinner on, off when we're done
    auto instrument = co_await stdexec::starts_on(workers, prepareInstrument(preset));
    sampler.install(instrument);                     // back on main
}
```

Built on `stdexec`, the reference implementation of C++26 `std::execution`.

## Try it

```sh
git clone https://github.com/konradglas/stdexec-affine
git clone https://github.com/NVIDIA/stdexec
git -C stdexec checkout fb944c333ae00d11cc0c56e1fb9b2bce04b5e162
cd stdexec-affine
clang++ -std=c++20 -Iinclude -isystem ../stdexec/include examples/loading_spinner.cpp -o spinner && ./spinner
```

Type a key: it loads on a worker while the spinner keeps spinning on main.
Type another one before it's done: the old load is cancelled, the new one wins.
`q` quits.

Then open `examples/loading_spinner.cpp` and break things.

## Who owns a task?

Every task you spawn has an owner that waits for it:

```cpp
class Plugin {
    affine::root_scope root{wakeup};   // wakeup posts to your main thread
    affine::task_handle loading;

    void onLoadPreset(Preset preset) {
        loading.close();               // stop the old load (just a request)
        loading = affine::spawn(loadPreset(preset), root);
    }
    ~Plugin() { root.close_and_wait(); }  // stop everything, wait until it's gone
};
```

## From the ADC talk

The demo from the talk, from callbacks to coroutines step by step:
[konradglas/adc-coroutines-demo](https://github.com/konradglas/adc-coroutines-demo).

Talks worth watching:

- Gor Nishanov, [Nano-coroutines to the Rescue!](https://www.youtube.com/watch?v=j9tlJAqMV7U) (CppCon 2018)
- Lewis Baker, [Structured Concurrency: Writing Safer Concurrent Code with Coroutines](https://www.youtube.com/watch?v=1Wy5sq3s2rg) (CppCon 2019)
- Goran Arandelovic, [Introduction to Sender Receiver framework and std::execution](https://www.youtube.com/watch?v=8V2bfeUZ31c) (using std::cpp)

## Use it

Header-only, C++20. With CMake, either point it at a stdexec checkout:

```sh
cmake -B build -DSTDEXEC_AFFINE_STDEXEC_SOURCE_DIR=/path/to/stdexec
cmake --build build && ctest --test-dir build
```

or `add_subdirectory` it into a project that already has `STDEXEC::stdexec` and link
`stdexec_affine::affine`.

Writing your own tasks? `AGENTS.md` has the rules of thumb (for people too).
