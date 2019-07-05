# Dependencies

| Name             | Version          | Platform |                              Optional |
| ---------------- | ---------------- | -------- | ------------------------------------- |
| autoconf         | >= 2.69          |    All   |                                    No |
| boost            | >= 1.65.0        |    All   |                                    No |
| clang            | 7.{0,1}          |    All   |                 Yes, if gcc installed |
| clang-format     | 7.{0,1}          |    All   |                      Yes (formatting) |
| clang-tidy       | 7.{0,1}          |    All   |                         Yes (linting) |
| cmake            | >= 3.9           |    All   |                                    No |
| gcc              | 8.{2,3}          |    All   | Yes, if clang installed, not for OS X |
| gcovr            | >= 3.2           |    All   |                        Yes (coverage) |
| graphviz         | any              |    All   |             Yes (query visualization) |
| libclang-dev     | 7.1              |    Linux |                             Yes (JIT) |
| libnuma-dev      | any              |    Linux |                            Yes (numa) |
| libnuma1         | any              |    Linux |                            Yes (numa) |
| llvm             | any              |    All   |                 Yes (code sanitizers) |
| llvm-7.0-tools   | 7                |    Linux |                                    No |
| parallel         | any              |    All   |                                   Yes |
| python           | >= 2.7 && < 3    |    All   |         Yes (linting and tests in CI) |
| pexpect          | >= 4             |    All   |                     Yes (tests in CI) |
| glob2            | >= 0.5           |    All   |                     Yes (tests in CI) |
| readline         | >= 7             |    All   |                                    No |
| sqlite3          | >= 3             |    All   |                                    No |
| tbb/libtbb-dev   | any              |    All   |                                    No |
| valgrind         | any              |    All   |            Yes, memory checking in CI |
| libpq-dev        | >= 9             |    All   |                                    No |
| systemtap        | any              |    Linux |                                    No |
| systemtap-sdt-dev| any              |    Linux |                                    No |


## Dependencies that are integrated in our build process via git submodules
- benchmark (https://github.com/google/benchmark)
- cxxopts (https://github.com/jarro2783/cxxopts.git)
- googletest (https://github.com/google/googletest)
- libpqxx (https://github.com/jtv/libpqxx)
- lz4 (https://github.com/lz4/lz4)
- sql-parser (https://github.com/hyrise/sql-parser)
- pgasus (https://github.com/kateyy/pgasus)
- cpp-btree (https://github.com/algorithm-ninja/cpp-btree)
- cqf (https://github.com/ArneMayer/cqf)
- jemalloc (https://github.com/jemalloc/jemalloc)
- zstd (https://github.com/facebook/zstd)
- tpcds-kit (https://github.com/hyrise-mp/tpcds-kit.git)
- tpcds-result-reproduction (https://github.com/hyrise-mp/tpcds-result-reproduction.git)
