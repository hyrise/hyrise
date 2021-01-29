[![Build Status](https://hyrise-ci.epic-hpi.de/buildStatus/icon?job=Hyrise/hyrise/master)](https://hyrise-ci.epic-hpi.de/blue/organizations/jenkins/hyrise%2Fhyrise/activity/)
[![Coverage Status](https://hyrise-coverage-badge.herokuapp.com/coverage_badge.svg)](https://hyrise-ci.epic-hpi.de/job/Hyrise/job/hyrise/job/master/lastStableBuild/Llvm-cov_5fReport/)
[![CodeFactor](https://www.codefactor.io/repository/github/hyrise/hyrise/badge)](https://www.codefactor.io/repository/github/hyrise/hyrise)

# Welcome to Hyrise

Hyrise is a research in-memory database system that has been developed [by HPI since 2009](https://www.vldb.org/pvldb/vol4/p105-grund.pdf) and has been entirely [rewritten in 2017](https://openproceedings.org/2019/conf/edbt/EDBT19_paper_152.pdf). Our goal is to provide a clean and flexible platform for research in the area of in-memory data management. Its architecture allows us, our students, and other researchers to conduct experiments around new data management concepts. To enable realistic experiments, Hyrise features comprehensive SQL support and performs powerful query plan optimizations. Well-known benchmarks, such as TPC-H or TPC-DS, can be executed with a single command and without any preparation.

This readme file focuses on the technical aspects of the repository. For more background on our research and for a list of publications, please visit the [Hyrise project page](https://hpi.de/plattner/projects/hyrise.html).

You can still find the (archived) previous version of Hyrise on [Github](https://github.com/hyrise/hyrise-v1).

## Citation

When referencing this version of Hyrise, please use the following bibtex entry:
<details><summary>(click to expand)</summary>
```
@inproceedings{DBLP:conf/edbt/DreselerK0KUP19,
  author    = {Markus Dreseler and
               Jan Kossmann and
               Martin Boissier and
               Stefan Klauck and
               Matthias Uflacker and
               Hasso Plattner},
  editor    = {Melanie Herschel and
               Helena Galhardas and
               Berthold Reinwald and
               Irini Fundulaki and
               Carsten Binnig and
               Zoi Kaoudi},
  title     = {Hyrise Re-engineered: An Extensible Database System for Research in
               Relational In-Memory Data Management},
  booktitle = {Advances in Database Technology - 22nd International Conference on
               Extending Database Technology, {EDBT} 2019, Lisbon, Portugal, March
               26-29, 2019},
  pages     = {313--324},
  publisher = {OpenProceedings.org},
  year      = {2019},
  url       = {https://doi.org/10.5441/002/edbt.2019.28},
  doi       = {10.5441/002/edbt.2019.28},
  timestamp = {Mon, 18 Mar 2019 16:09:00 +0100},
  biburl    = {https://dblp.org/rec/conf/edbt/DreselerK0KUP19.bib},
  bibsource = {dblp computer science bibliography, https://dblp.org}
}
```
</details>

## Supported Systems
Hyrise is developed for Linux (preferrably the most current Ubuntu version) and optimized to run on server hardware. We support Mac to facilitate the local development of Hyrise, but do not recommend it for benchmarking.

## Supported Benchmarks
We support a number of benchmarks out of the box. This makes it easy to generate performance numbers without having to set up the data generation, loading CSVs, and finding a query runner. You can run them using the `./hyriseBenchmark*` binaries.

| Benchmark  | Notes                                                                                                                    |
| ---------- | ------------------------------------------------------------------------------------------------------------------------ |
| TPC-DS     | [Query Plans](https://hyrise-ci.epic-hpi.de/job/hyrise/job/hyrise/job/master/lastStableBuild/artifact/query_plans/tpcds) |
| TPC-H      | [Query Plans](https://hyrise-ci.epic-hpi.de/job/hyrise/job/hyrise/job/master/lastStableBuild/artifact/query_plans/tpch)  |
| JCC-H      | Call the hyriseBenchmarkTPCH binary with the -j flag.                                                                    | 
| TPC-C      | In development, no proper optimization done yet                                                                          |
| Join Order |                                                                                                                          |

# Getting started

*Have a look at our [contributor guidelines](CONTRIBUTING.md)*.

You can find definitions of most of the terms and abbreviations used in the code in the [glossary](GLOSSARY.md). If you cannot find something that you are looking for, feel free to open an issue.

The [Step by Step Guide](https://github.com/hyrise/hyrise/wiki/Step-by-Step-Guide) is a good starting point to get to know Hyrise.

## Native Setup
You can install the dependencies on your own or use the install_dependencies.sh script (**recommended**) which installs all of the therein listed dependencies and submodules.
The install script was tested under macOS Big Sur (10.16) and Ubuntu 20.10 (apt-get).

See [dependencies](DEPENDENCIES.md) for a detailed list of dependencies to use with `brew install` or `apt-get install`, depending on your platform. As compilers, we generally use the most recent version of clang and gcc (Linux only). Please make sure that the system compiler points to the most recent version or use cmake (see below) accordingly.
Older versions may work, but are neither tested nor supported.

## Setup using Docker
If you want to create a Docker-based development environment using CLion, head over to our [dedicated tutorial](https://github.com/hyrise/hyrise/wiki/Use-Docker-with-CLion). 

Otherwise, to get all dependencies of Hyrise into a Docker image, run
```
docker build -t hyrise .
```

You can start the container via
```
docker run -it hyrise
```

Inside the container, you can then checkout Hyrise and run `./install_dependencies.sh` to download the required submodules.

## Building and Tooling
It is highly recommended to perform out-of-source builds, i.e., creating a separate directory for the build.
Advisable names for this directory would be `cmake-build-{debug,release}`, depending on the build type.
Within this directory call `cmake ..` to configure the build.
By default, we use very strict compiler flags (beyond `-Wextra`, including `-Werror`). If you use one of the officially supported environments, this should not be an issue. If you simply want to test Hyrise on a different system and run into issues, you can call `cmake -DHYRISE_RELAXED_BUILD=On ..`, which will disable these strict checks.
Subsequent calls to CMake, e.g., when adding files to the build will not be necessary, the generated Makefiles will take care of that.

### Compiler choice
CMake will default to your system's default compiler.
To use a different one, call `cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ ..` in a clean build directory. See [dependencies](DEPENDENCIES.md) for supported compiler versions.

### Unity Builds
Starting with cmake 3.16, you can use `-DCMAKE_UNITY_BUILD=On` to perform unity builds. For a complete (re-)build or when multiple files have to be rebuilt, these are usually faster, as the relative cost of starting a compiler process and loading the most common headers is reduced. However, this only makes sense for debug builds. See our [blog post](https://medium.com/hyrise/reducing-hyrises-build-time-8523135aed72) on reducing the compilation time for details.

### ccache
For development, you may want to use [ccache](https://ccache.samba.org/), which reduces the time needed for recompiles significantly. Especially when switching branches, this can reduce the time to recompile from several minutes to one or less. On the downside, we have seen random build failures on our CI server, which is why we do not recommend ccache anymore but merely list it as an option. To use ccache, add `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache` to your cmake call. You will need to [adjust some ccache settings](https://ccache.dev/manual/latest.html#_precompiled_headers) either in your environment variables or in your [ccache config](https://ccache.dev/manual/latest.html#_configuration) so that ccache can handle the precompiled headers. On our CI server, this worked for us: `CCACHE_SLOPPINESS=file_macro,pch_defines,time_macros CCACHE_DEPEND=1`.

### Build
Simply call `make -j*`, where `*` denotes the number of threads to use.

Usually debug binaries are created.
To configure a build directory for a release build make sure it is empty and call CMake like `cmake -DCMAKE_BUILD_TYPE=Release`

### Lint
`./scripts/lint.sh` (Google's cpplint is used for the database code. In addition, we use _flake8_ for linting the Python scripts under /scripts.)

### Format
`./scripts/format.sh` (clang-format is used for the database code. We use _black_ for formatting the Python scripts under /scripts.)

### Test
Calling `make hyriseTest` from the build directory builds all available tests.
The binary can be executed with `./<YourBuildDirectory>/hyriseTest`.
Note, that the tests/sanitizers/etc need to be executed from the project root in order for table files to be found.

### Coverage
`./scripts/coverage.sh` will print a summary to the command line and create detailed html reports at ./coverage/index.html

*Supports only clang on MacOS and only gcc on linux*

### Address/UndefinedBehavior Sanitizers
`cmake -DENABLE_ADDR_UB_SANITIZATION=ON` will generate Makefiles with AddressSanitizer and Undefined Behavior options.
Compile and run them as normal - if any issues are detected, they will be printed to the console.
It will fail on the first detected error and will print a summary.
To convert addresses to actual source code locations, make sure llvm-symbolizer is installed (included in the llvm package) and is available in `$PATH`.
To specify a custom location for the symbolizer, set `$ASAN_SYMBOLIZER_PATH` to the path of the executable.
This seems to work out of the box on macOS - If not, make sure to have llvm installed.
The binary can be executed with `LSAN_OPTIONS=suppressions=asan-ignore.txt ./<YourBuildDirectory>/hyriseTest`.

`cmake -DENABLE_THREAD_SANITIZATION=ON` will work as above but with the ThreadSanitizer. Some sanitizers are mutually exclusive, which is why we use two configurations for this.

### Compile Times
When trying to optimize the time spent building the project, it is often helpful to have an idea how much time is spent where.
`scripts/compile_time.sh` helps with that. Get usage instructions by running it without any arguments.

## Maintainers

- Jan Kossmann
- Markus Dreseler
- Martin Boissier
- Stefan Halfpap


Contact: firstname.lastname@hpi.de

## Contributors

-   Yannick   Bäumer
-   Lawrence  Benson
-   Timo      Djürken
-   Alexander Dubrawski
-   Fabian    Dumke
-   Leonard   Geier
-   Richard   Ebeling
-   Fabian    Engel
-   Moritz    Eyssen
-   Martin    Fischer
-   Christian Flach
-   Pedro     Flemming
-   Mathias   Flüggen
-   Johannes  Frohnhofen
-   Pascal    Führlich
-   Carl      Gödecken
-   Adrian    Holfter
-   Sven      Ihde
-   Ivan      Illic
-   Jonathan  Janetzki
-   Michael   Janke
-   Max       Jendruk
-   David     Justen
-   Youri     Kaminsky
-   Marvin    Keller
-   Mirko     Krause
-   Eva       Krebs
-   Sven      Lehmann
-   Till      Lehmann
-   Tom       Lichtenstein
-   Daniel    Lindner
-   Alexander Löser
-   Jan       Mattfeld
-   Arne      Mayer
-   Dominik   Meier
-   Julian    Menzler
-   Torben    Meyer
-   Leander   Neiß
-   Hendrik   Rätz
-   Alexander Riese
-   Johannes  Schneider
-   David     Schumann
-   Simon     Siegert
-   Arthur    Silber
-   Toni      Stachewicz
-   Daniel    Stolpe
-   Jonathan  Striebel
-   Nils      Thamm
-   Hendrik   Tjabben
-   Justin    Trautmann
-   Carsten   Walther
-   Marcel    Weisgut
-   Lukas     Wenzel
-   Fabian    Wiebe
-   Tim       Zimmermann
