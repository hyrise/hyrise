# Dependencies

| Name                      | Version          | Platform |                              Optional |
| ------------------------- | ---------------- | -------- | ------------------------------------- |
| autoconf                  | >= 2.69          |    All   |                                    No |
| boost                     | >= 1.70.0        |    All   |                                    No |
| clang                     | >= 11.0          |    All   |                 Yes, if gcc installed |
| clang-format              | >= 11.0          |    All   |                      Yes (formatting) |
| clang-tidy                | >= 11.0          |    All   |                         Yes (linting) |
| coreutils                 | any              |    Mac   |                         Yes (scripts) |
| cmake                     | >= 3.18          |    All   |                                    No |
| dos2unix                  | any              |    All   |                         Yes (linting) |
| gcc                       | >= 9.1           |    All   | Yes, if clang installed, not for OS X |
| gcovr                     | >= 3.2           |    All   |                        Yes (coverage) |
| graphviz                  | any              |    All   |             Yes (query visualization) |
| libnuma-dev               | any              |    Linux |                            Yes (numa) |
| libnuma1                  | any              |    Linux |                            Yes (numa) |
| libpq-dev                 | >= 9             |    All   |                                    No |
| lld                       | any              |    Linux |                                   Yes |
| parallel                  | any              |    All   |                                   Yes |
| pexpect                   | >= 4             |    All   |                     Yes (tests in CI) |
| postgresql-server-dev-all | >= 154           |    Linux |                                    No |
| python                    | >= 3.6           |    All   |         Yes (linting and tests in CI) |
| readline                  | >= 7             |    All   |                                    No |
| sqlite3                   | >= 3             |    All   |                                    No |
| tbb/libtbb-dev            | any              |    All   |                                    No |
| valgrind                  | any              |    All   |            Yes, memory checking in CI |


For dependencies that are integrated in our build process via git submodules, please check .gitmodules
