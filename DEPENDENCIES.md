# Dependencies

| Name                      | Version          | Platform |                              Optional |
| ------------------------- | ---------------- | -------- | ------------------------------------- |
| autoconf                  | >= 2.69          |    All   |                                    No |
| boost                     | >= 1.65.0        |    All   |                                    No |
| clang                     | >= 9.0           |    All   |                 Yes, if gcc installed |
| clang-format              | >= 9.0           |    All   |                      Yes (formatting) |
| clang-tidy                | >= 9.0           |    All   |                         Yes (linting) |
| cmake                     | >= 3.9           |    All   |                                    No |
| gcc                       | >= 9.1           |    All   | Yes, if clang installed, not for OS X |
| gcovr                     | >= 3.2           |    All   |                        Yes (coverage) |
| graphviz                  | any              |    All   |             Yes (query visualization) |
| libnuma-dev               | any              |    Linux |                            Yes (numa) |
| libnuma1                  | any              |    Linux |                            Yes (numa) |
| libpq-dev                 | >= 9             |    All   |                                    No |
| lld                       | any              |    Linux |   No, but could be removed from cmake |
| parallel                  | any              |    All   |                                   Yes |
| pexpect                   | >= 4             |    All   |                     Yes (tests in CI) |
| postgresql-server-dev-all | >= 154           |    Linux |                                    No |
| python                    | >= 2.7 && < 3    |    All   |         Yes (linting and tests in CI) |
| readline                  | >= 7             |    All   |                                    No |
| sqlite3                   | >= 3             |    All   |                                    No |
| systemtap                 | any              |    Linux |                                    No |
| systemtap-sdt-dev         | any              |    Linux |                                    No |
| tbb/libtbb-dev            | any              |    All   |                                    No |
| valgrind                  | any              |    All   |            Yes, memory checking in CI |


## Dependencies that are integrated in our build process via git submodules
- benchmark (https://github.com/google/benchmark)
- cpp-btree (https://github.com/algorithm-ninja/cpp-btree)
- cpplint (https://github.com/cpplint/cpplint.git)
- cqf (https://github.com/ArneMayer/cqf)
- cxxopts (https://github.com/jarro2783/cxxopts.git)
- flash_hash_map (https://github.com/skarupke/flat_hash_map)
- googletest (https://github.com/google/googletest)
- jemalloc (https://github.com/jemalloc/jemalloc)
- join-order-benchmark (https://github.com/gregrahn/join-order-benchmark)
- libpqxx (https://github.com/jtv/libpqxx)
- lz4 (https://github.com/lz4/lz4)
- nlohmann_json (https://github.com/nlohmann/json)
- pgasus (https://github.com/kateyy/pgasus)
- sql-parser (https://github.com/hyrise/sql-parser)
- tpcds-kit (https://github.com/hyrise-mp/tpcds-kit.git)
- tpcds-result-reproduction (https://github.com/hyrise-mp/tpcds-result-reproduction.git)
- zstd (https://github.com/facebook/zstd)
