# Dependencies

| Name                      | Version          | Platform |                              Optional |
| ------------------------- | ---------------- | -------- | ------------------------------------- |
| autoconf                  | >= 2.69          |    All   |                                    No |
| bc                        | any              |    All   |                  Yes (coverage in CI) |
| boost                     | >= 1.81.0        |    All   |                                    No |
| clang                     | >= 14.0          |    All   |                 Yes, if gcc installed |
| clang-format              | >= 17.0          |    All   |                      Yes (formatting) |
| clang-tidy                | >= 17.0          |    All   |                         Yes (linting) |
| coreutils                 | any              |    Mac   |                         Yes (scripts) |
| cmake                     | >= 3.18          |    All   |                                    No |
| dos2unix                  | any              |    All   |                         Yes (linting) |
| gcc                       | >= 11.4          |    All   | Yes, if clang installed, not for OS X |
| graphviz                  | any              |    All   |             Yes (query visualization) |
| libnuma-dev               | any              |    Linux |                            Yes (numa) |
| libnuma1                  | any              |    Linux |                            Yes (numa) |
| libpq-dev                 | >= 9             |    All   |                                    No |
| lld                       | any              |    Linux |     Yes (needed for clang LTO builds) |
| parallel                  | any              |    All   |                                   Yes |
| pexpect                   | >= 4             |    All   |                     Yes (tests in CI) |
| postgresql-server-dev-all | >= 154           |    Linux |                                    No |
| python                    | >= 3.6           |    All   |         Yes (linting and tests in CI) |
| readline                  | >= 7             |    All   |                                    No |
| sqlite3                   | >= 3             |    All   |                                    No |
| tbb/libtbb-dev (oneTBB)   | any              |    All   |                                    No |
| valgrind                  | >= 3.20          |    All   |           Yes (memory checking in CI) |

For dependencies that are integrated in our build process via git submodules, please check .gitmodules.
