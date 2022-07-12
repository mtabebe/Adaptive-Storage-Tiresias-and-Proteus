This directory contains all the unit tests for this repository.

Follow the example of the [counter locks test](counter_lock.cpp) for an example of how to add a new [gtest](http://gflags.github.io/gflags/)

To run the [unit tests](tests/test.cpp) do the following:

```
cd Adapt-HTAP/code;
sudo gradle build; 
# alternatively to just compile debug non-optimized binary run
#  gradleTasks=$(sudo gradle tasks | grep DebugNonOpt | cut -d “-” -f 1 | paste -sd “” -); sudo gradle $gradleTasks
./build/exe/unitTest/debugNonOpt/unitTest;
```

Use the `--help` flag to see all the available configuration options.

Some helpful options are `--gtest_filter=` which allows filtering out the unit tests with regexes.
