import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')

// Due to their long runtime, we skip several tests in sanitizer and MacOS builds.
tests_excluded_in_sanitizer_builds = 'SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCountsAndTableConstraints:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:SSBTableGeneratorTest.GenerateAndStoreRowCounts'

tests_excluded_in_mac_builds = 'TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:SSBTableGeneratorTest.GenerateAndStoreRowCounts'

// We run the strict ("more aggressive") checks for the address sanitizer
// (see https://github.com/google/sanitizers/wiki/AddressSanitizer#faq). Moreover, we activate the leak sanitizer.
asan_options = 'strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1'

try {
  node {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team) or the Jenkins admin user
      def cause = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0]
      def jenkinsUserName = cause ? cause['userId'] : null

      if (jenkinsUserName != "admin" && env.BRANCH_NAME != "master") {
        try {
          withCredentials([usernamePassword(credentialsId: 'github', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
            env.PR_CREATED_BY = pullRequest.createdBy
            sh '''
              curl -s -I -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/hyrise/hyrise/collaborators/${PR_CREATED_BY} | head -n 1 | grep "204"
            '''
          }
        } catch (error) {
          stage ("User unknown.") {
            script {
              githubNotify context: 'CI Pipeline', status: 'FAILURE', description: 'User is not a collaborator.'
            }
          }
          throw error
        }
      }

      script {
        githubNotify context: 'CI Pipeline', status: 'PENDING'

        // Cancel previous builds
        if (env.BRANCH_NAME != 'master') {
          def jobname = env.JOB_NAME
          def buildnum = env.BUILD_NUMBER.toInteger()
          def job = Jenkins.instance.getItemByFullName(jobname)
          for (build in job.builds) {
            if (!build.isBuilding()) { continue; }
            if (buildnum == build.getNumber().toInteger()) { continue; }
            echo "Cancelling previous build " + build.getNumber().toString()
            build.doStop();
          }
        }
      }
    }
  }

  node('linux') {
    stage("Hostname") {
      // Print the hostname to let us know on which node the docker image was executed for reproducibility.
      sh "hostname"
    }

    // The empty '' results in using the default registry: https://index.docker.io/v1/
    docker.withRegistry('', 'docker') {
      def hyriseCI = docker.image('hyrise/hyrise-ci:24.04');
      hyriseCI.pull()

      // LSAN (executed as part of ASAN) requires elevated privileges. Therefore, we had to add --cap-add SYS_PTRACE.
      // Even if the CI run sometimes succeeds without SYS_PTRACE, you should not remove it until you know what you are
      // doing. See also: https://github.com/google/sanitizers/issues/764
      hyriseCI.inside("--cap-add SYS_PTRACE -u 0:0") {
        try {
          stage("Setup") {
            checkout scm

            // Check if ninja-build is installed. As make is sufficient to work with Hyrise, ninja-build is not
            // installed via install_dependencies.sh but is part of the hyrise-ci docker image.
            sh "ninja --version > /dev/null"

            // During CI runs, the user is different from the owner of the directories, which blocks the execution of
            // git commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be
            // executed if the corresponding directories are added as safe directories.
            sh '''
            git config --global --add safe.directory $WORKSPACE
            # Get the paths of the submodules; for each path, add it as a git safe.directory
            grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
            '''

            sh "./install_dependencies.sh"

            cmake = 'cmake -DCI_BUILD=ON'

            // We do not use unity builds with clang-tidy (see below).
            unity = '-DCMAKE_UNITY_BUILD=ON'

            // To speed compiling up, we use ninja for most builds.
            ninja = '-GNinja'

            // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
            // support older versions. We test LLVM 16 (older versions might work, but LLVM 15 has issues with libstdc++
            // of GCC 14) and GCC 13.2 (oldest version supported by Hyrise). We execute at least debug runs for them. If
            // you want to upgrade compiler versions, please update install_dependencies.sh, DEPENDENCIES.md, and the
            // documentation (README, Wiki).
            clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
            clang16 = '-DCMAKE_C_COMPILER=clang-16 -DCMAKE_CXX_COMPILER=clang++-16'
            gcc = '-DCMAKE_C_COMPILER=gcc-14 -DCMAKE_CXX_COMPILER=g++-14'
            gcc13 = '-DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13'

            debug = '-DCMAKE_BUILD_TYPE=Debug'
            release = '-DCMAKE_BUILD_TYPE=Release'
            relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

            // LTO is automatically disabled for debug and sanitizer builds. As LTO linking with GCC and gold (lld is
            // not supported when using GCC and LTO) takes over an hour, we skip LTO for the GCC release build.
            no_lto = '-DNO_LTO=TRUE'

            // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the
            // same time, we run into conflicts. Thus, run this one (any one, really) first, so that the autoconf step
            // can finish in peace.
            sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity} .. && make -j \$(nproc) libjemalloc-build"

            // Configure the rest in parallel. We use unity builds to decrease build times. The only exception is the
            // clang-tidy build as it might otherwise miss some issues (e.g., missing includes).
            sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}                      ${ninja} -DENABLE_CLANG_TIDY=ON .. &\
            mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity}           ${ninja} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
            mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}   ${unity}           ${ninja} -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
            mkdir clang-debug-addr-ub-leak-sanitizers && cd clang-debug-addr-ub-leak-sanitizers &&       ${cmake} ${debug}          ${clang}   ${unity}           ${ninja} -DENABLE_ADDR_UB_LEAK_SANITIZATION=ON .. &\
            mkdir clang-release-addr-ub-leak-sanitizers && cd clang-release-addr-ub-leak-sanitizers &&   ${cmake} ${release}        ${clang}   ${unity}           ${ninja} -DENABLE_ADDR_UB_LEAK_SANITIZATION=ON .. &\
            mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}   ${unity}           ${ninja} -DENABLE_THREAD_SANITIZATION=ON .. &\
            mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}   ${unity}           ${ninja} .. &\
            mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}     ${unity}           ${ninja} .. &\
            mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}     ${unity} ${no_lto} ${ninja} .. &\
            mkdir clang-16-debug && cd clang-16-debug &&                                                 ${cmake} ${debug}          ${clang16} ${unity}           ${ninja} .. &\
            mkdir gcc-13-debug && cd gcc-13-debug &&                                                     ${cmake} ${debug}          ${gcc13}   ${unity}           ${ninja} .. &\
            wait"
          }

          parallel clangDebug: {
            stage("clang-debug") {
              // We build clang-debug using make to test make once (and clang-debug is the fastest build).
              sh "cd clang-debug && make all -j \$(( \$(nproc) / 4))"
              sh "./clang-debug/hyriseTest clang-debug"
            }
          }, clang16Debug: {
            stage("clang-16-debug") {
              sh "cd clang-16-debug && ninja all -j \$(( \$(nproc) / 4))"
              sh "./clang-16-debug/hyriseTest clang-16-debug"
            }
          }, gccDebug: {
            stage("gcc-debug") {
              sh "cd gcc-debug && ninja all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-debug && ./hyriseTest"
            }
          }, gcc13Debug: {
            stage("gcc-13-debug") {
              sh "cd gcc-13-debug && ninja all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-13-debug && ./hyriseTest"
            }
          }, lint: {
            stage("Linting") {
              sh "scripts/lint.sh"
            }
          }

          // We distribute the cores to processes in a way to even the running times. With an even distributions,
          // clang-tidy builds take up to 3h (galileo server). In addition to compile time, the distribution also
          // considers the long test runtimes of clangRelWithDebInfoThreadSanitizer (~50 minutes).
          parallel clangRelease: {
            stage("clang-release") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release && ninja all -j \$(( \$(nproc) / 10))"
                sh "./clang-release/hyriseTest clang-release"
                sh "./clang-release/hyriseSystemTest clang-release"
                sh "./scripts/test/hyriseConsole_test.py clang-release"
                sh "./scripts/test/hyriseServer_test.py clang-release"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-release"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
                sh "cd clang-release && ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
                sh "cd clang-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization

              } else {
                Utils.markStageSkippedForConditional("clangRelease")
              }
            }
          }, debugSystemTests: {
            stage("system-tests") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir clang-debug-system &&  ./clang-debug/hyriseSystemTest clang-debug-system"
                sh "mkdir gcc-debug-system &&  ./gcc-debug/hyriseSystemTest gcc-debug-system"
                sh "./scripts/test/hyriseConsole_test.py clang-debug"
                sh "./scripts/test/hyriseServer_test.py clang-debug"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-debug"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate cached data
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkStarSchema_test.py ." // Own folder to isolate cached data
                sh "./scripts/test/hyriseConsole_test.py gcc-debug"
                sh "./scripts/test/hyriseServer_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-debug"
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate cached data
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkStarSchema_test.py ." // Own folder to isolate cached data

              } else {
                Utils.markStageSkippedForConditional("debugSystemTests")
              }
            }
          }, clangDebugRunShuffled: {
            stage("clang-debug:test-shuffle") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-debug/run-shuffled"
                sh "./clang-debug/hyriseTest clang-debug/run-shuffled --gtest_repeat=5 --gtest_shuffle"
                // We do not want to trigger SSB data generation concurrently since it is not concurrency-safe.
                sh "./clang-debug/hyriseSystemTest clang-debug/run-shuffled --gtest_repeat=2 --gtest_shuffle --gtest_filter=\"-SSBTableGeneratorTest.*:SQLiteTestRunnerEncodings/*\""
              } else {
                Utils.markStageSkippedForConditional("clangDebugRunShuffled")
              }
            }
          }, clangDebugUnityODR: {
            stage("clang-debug-unity-odr") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if unity builds work even if everything is batched into a single compilation unit. This helps prevent ODR (one definition rule) issues.
                sh "cd clang-debug-unity-odr && ninja all -j 2"
              } else {
                Utils.markStageSkippedForConditional("clangDebugUnityODR")
              }
            }
          }, clangDebugTidy: {
            stage("clang-debug:tidy") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // We do not run tidy checks on the src/test folder, so there is no point in running the expensive clang-tidy for those files
                sh "cd clang-debug-tidy && ninja hyrise_impl hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer hyriseMvccDeletePlugin hyriseUccDiscoveryPlugin -k 0 -j \$(( \$(nproc) / 5))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugTidy")
              }
            }
          }, clangDebugDisablePrecompileHeaders: {
            stage("clang-debug:disable-precompile-headers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if builds work even when precompile headers is turned off. Executing the binaries is unnecessary as the observed errors are missing includes.
                sh "cd clang-debug-disable-precompile-headers && ninja hyriseTest hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k 0 -j 2"
              } else {
                Utils.markStageSkippedForConditional("clangDebugDisablePrecompileHeaders")
              }
            }
          }, clangDebugAddrUBLeakSanitizers: {
            stage("clang-debug:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-debug-addr-ub-leak-sanitizers && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 20))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-debug-addr-ub-leak-sanitizers/hyriseTest clang-debug-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-debug-addr-ub-leak-sanitizers/hyriseSystemTest --gtest_filter=-${tests_excluded_in_sanitizer_builds} clang-debug-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-debug-addr-ub-leak-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 1"
              } else {
                Utils.markStageSkippedForConditional("clangDebugAddrUBLeakSanitizers")
              }
            }
          }, gccRelease: {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              stage("gcc-release") {
                sh "cd gcc-release && ninja all -j \$(( \$(nproc) / 10))"
                sh "./gcc-release/hyriseTest gcc-release"
                sh "./gcc-release/hyriseSystemTest gcc-release"
                sh "./scripts/test/hyriseConsole_test.py gcc-release"
                sh "./scripts/test/hyriseServer_test.py gcc-release"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-release"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-release"
                sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
                sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
              }
            } else {
                Utils.markStageSkippedForConditional("gccRelease")
            }
          }, clangReleaseAddrUBLeakSanitizers: {
            stage("clang-release:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release-addr-ub-leak-sanitizers && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 4))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-release-addr-ub-leak-sanitizers/hyriseTest clang-release-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-release-addr-ub-leak-sanitizers/hyriseSystemTest --gtest_filter=-${tests_excluded_in_sanitizer_builds} clang-release-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ./clang-release-addr-ub-leak-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10 --cores \$(( \$(nproc) / 10))"
                sh "cd clang-release-addr-ub-leak-sanitizers && LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=\${asan_options} ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
              } else {
                Utils.markStageSkippedForConditional("clangReleaseAddrUBLeakSanitizers")
              }
            }
          }, clangRelWithDebInfoThreadSanitizer: {
            stage("clang-relwithdebinfo:thread-sanitizer") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-relwithdebinfo-thread-sanitizer && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC hyriseBenchmarkTPCDS -j \$(( \$(nproc) / 5))"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseTest clang-relwithdebinfo-thread-sanitizer"
                // We exclude tests matching NodeQueueSchedulerSemaphoreIncrements* as those tests were stuck with TSAN.
                // We have seen runtimes of over 20h. The (much larger) tests running in Release mode did not show any
                // issues when run for several days. We thus consider it a TSAN issue.
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseSystemTest --gtest_filter=-${tests_excluded_in_sanitizer_builds}:NodeQueueSchedulerSemaphoreIncrements* clang-relwithdebinfo-thread-sanitizer"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseBenchmarkTPCH -s .01 -r 100 --scheduler --clients 10 --cores \$(( \$(nproc) / 10))"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseBenchmarkTPCC -s 1 --time 120 --scheduler --clients 10 --cores \$(( \$(nproc) / 10))"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseBenchmarkTPCDS -s 1 -r 5 --scheduler --clients 5 --cores \$(( \$(nproc) / 10))"
              } else {
                Utils.markStageSkippedForConditional("clangRelWithDebInfoThreadSanitizer")
              }
            }
          }, clangDebugCoverage: {
            stage("clang-debug-coverage") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "./scripts/coverage.sh --generate_badge=true"
                sh "find coverage -type d -exec chmod +rx {} \\;"
                archive 'coverage_badge.svg'
                archive 'coverage_percent.txt'
                publishHTML (target: [
                  allowMissing: false,
                  alwaysLinkToLastBuild: false,
                  keepAll: true,
                  reportDir: 'coverage',
                  reportFiles: 'index.html',
                  reportName: "Llvm-cov_Report"
                ])
                script {
                  coverageChange = sh script: "./scripts/compare_coverage.sh", returnStdout: true
                  githubNotify context: 'Coverage', description: "$coverageChange", status: 'SUCCESS', targetUrl: "${env.BUILD_URL}/RCov_20Report/index.html"
                }
              } else {
                Utils.markStageSkippedForConditional("clangDebugCoverage")
              }
            }
          }

          parallel memcheckReleaseTest: {
            stage("memcheckReleaseTest") {
              // Runs after the other sanitizers as it depends on clang-release to be built.
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-release-memcheck-test"
                // If this shows a leak, try --leak-check=full, which is slower but more precise. Valgrind serializes
                // concurrent threads into a single one. Thus, we limit the cores and data preparation cores for the
                // benchmark runs to reduce this serialization overhead. According to the Valgrind documentation,
                // --fair-sched=yes "improves overall responsiveness if you are running an interactive multithreaded
                // program" and "produces better reproducibility of thread scheduling for different executions of a
                // multithreaded application" (i.e., there are no runs that are randomly faster or slower).
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --fair-sched=yes --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseTest clang-release-memcheck-test --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate:SQLiteTestRunner*"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --fair-sched=yes --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCH -s .01 -r 1 --scheduler --cores 4 --data_preparation_cores 4"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --fair-sched=yes --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCDS -s 1 -r 1 --scheduler --cores 4 --data_preparation_cores 4"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --fair-sched=yes --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCC -s 1 --scheduler --cores 4 --data_preparation_cores 4"
              } else {
                Utils.markStageSkippedForConditional("memcheckReleaseTest")
              }
            }
          }, tpchVerification: {
            stage("tpchVerification") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Verify both single- and multithreaded results.
                sh "./clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 1 -s 1 --verify"
                sh "./clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 1 -s 1 --verify --scheduler --clients 1 --cores 10"
              } else {
                Utils.markStageSkippedForConditional("tpchVerification")
              }
            }
          }, tpchQueryPlans: {
            stage("tpchQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/tpch; cd query_plans/tpch && ../../clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 2 -s 10 --visualize && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/tpch/*.svg'
                archiveArtifacts artifacts: 'query_plans/tpch/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("tpchQueryPlans")
              }
            }
          }, tpcdsQueryPlansAndVerification: {
            stage("tpcdsQueryPlansAndVerification") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/tpcds; cd query_plans/tpcds && ln -s ../../resources; ../../clang-release/hyriseBenchmarkTPCDS --dont_cache_binary_tables -r 1 -s 1 --visualize --verify && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/tpcds/*.svg'
                archiveArtifacts artifacts: 'query_plans/tpcds/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("tpcdsQueryPlansAndVerification")
              }
            }
          }, jobQueryPlans: {
            stage("jobQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // In contrast to TPC-H and TPC-DS above, we execute the JoinOrderBenchmark from the project's root directoy because its setup script requires us to do so.
                sh "mkdir -p query_plans/job && ./clang-release/hyriseBenchmarkJoinOrder --dont_cache_binary_tables -r 1 --visualize && ./scripts/plot_operator_breakdown.py ./clang-release/ && mv operator_breakdown.pdf query_plans/job && mv *QP.svg query_plans/job"
                archiveArtifacts artifacts: 'query_plans/job/*.svg'
                archiveArtifacts artifacts: 'query_plans/job/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("jobQueryPlans")
              }
            }
          }, ssbQueryPlans: {
            stage("ssbQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/ssb; cd query_plans/ssb && ../../clang-release/hyriseBenchmarkStarSchema --dont_cache_binary_tables -r 1 -s 1 --visualize && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/ssb/*.svg'
                archiveArtifacts artifacts: 'query_plans/ssb/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("ssbQueryPlans")
              }
            }
          }, nixSetup: {
            stage('nixSetup') {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "curl -L https://nixos.org/nix/install > nix-install.sh && chmod +x nix-install.sh && ./nix-install.sh --daemon --yes"
                sh "/nix/var/nix/profiles/default/bin/nix-shell resources/nix --pure --run \"mkdir nix-debug && cd nix-debug && cmake ${debug} ${clang} ${unity} ${ninja} .. && ninja all -j \$(( \$(nproc) / 7)) && ./hyriseTest\""
                // We allocate a third of all cores for the release build as several parallel stages should have already finished at this point.
                sh "/nix/var/nix/profiles/default/bin/nix-shell resources/nix --pure --run \"mkdir nix-release && cd nix-release && cmake ${release} ${clang} ${unity} ${ninja} .. && ninja all -j \$(( \$(nproc) / 3)) && ./hyriseTest\""
              } else {
                Utils.markStageSkippedForConditional("nixSetup")
              }
            }
          }
        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
        }
      }
    }
  }

  parallel clangMacX64: {
    node('mac') {
      stage("clangMacX64") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            // We have experienced frequent network problems with this CI machine. So far, we have not found the cause.
            // Since we run this stage very late and it frequently fails due to network problems, we retry pulling the
            // sources five times to avoid re-runs of entire Full CI runs that failed in the very last stage due to a
            // single network issue.
            retry(5) {
              try {
                checkout scm

                // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container.
                sh "git submodule update --init --recursive --jobs 4 --depth=1"
              } catch (error) {
                sh "ls -A1 | xargs rm -rf"
                throw error
              }
            }

            // Build hyriseTest (Debug) with macOS's default compiler (Apple clang) and run it. Passing clang
            // explicitly seems to make the compiler find C system headers (required for SSB and JCC-H data generators)
            // that are not found otherwise.
            sh "mkdir clang-apple-debug && cd clang-apple-debug && cmake ${debug} ${unity} ${ninja} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
            sh "cd clang-apple-debug && ninja"
            sh "./clang-apple-debug/hyriseTest"

            // Build Hyrise (Release) with a recent clang compiler version (as recommended for Hyrise on macOS) and run
            // various tests. As the release build already takes quite a while on the Intel machine, we disable LTO and
            // only build release with LTO on the ARM machine.
            sh "mkdir clang-release && cd clang-release && cmake ${release} ${ninja} ${unity} ${no_lto} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@19/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@19/bin/clang++ .."
            sh "cd clang-release && ninja"
            sh "./clang-release/hyriseTest"
            sh "./clang-release/hyriseSystemTest --gtest_filter=-${tests_excluded_in_mac_builds}"
            sh "./scripts/test/hyriseConsole_test.py clang-release"
            sh "./scripts/test/hyriseServer_test.py clang-release"
            sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangMacX64")
        }
      }
    }
  }, clangMacArm: {
    // For this to work, we installed a native non-standard JDK (zulu) via brew. See #2339 for more details.
    node('mac-arm') {
      stage("clangMacArm") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm

            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"

            // Build hyriseTest (Release) with macOS's default compiler (Apple clang) and run it. Passing clang
            // explicitly seems to make the compiler find C system headers (required for SSB and JCC-H data generators)
            // that are not found otherwise.
            sh "mkdir clang-apple-release && cd clang-apple-release && cmake ${release} ${ninja} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
            sh "cd clang-apple-release && ninja"
            sh "./clang-apple-release/hyriseTest"

            // Build Hyrise (Debug) with a recent clang compiler version (as recommended for Hyrise on macOS) and run
            // various tests.
            // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
            sh "mkdir clang-debug && cd clang-debug && cmake ${debug} ${unity} ${ninja} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@19/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@19/bin/clang++ .."
            sh "cd clang-debug && ninja"

            // Check whether arm64 binaries are built to ensure that we are not accidentally running rosetta that
            // executes x86 binaries on arm.
            sh "file ./clang-debug/hyriseTest | grep arm64"

            sh "./clang-debug/hyriseTest"
            sh "./clang-debug/hyriseSystemTest --gtest_filter=-${tests_excluded_in_mac_builds}"
            sh "./scripts/test/hyriseConsole_test.py clang-debug"
            sh "./scripts/test/hyriseServer_test.py clang-debug"
            sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangMacArm")
        }
      }
    }
  }

  node {
    stage("Notify") {
      script {
        githubNotify context: 'CI Pipeline', status: 'SUCCESS'
        if (env.BRANCH_NAME == 'master' || full_ci) {
          githubNotify context: 'Full CI', status: 'SUCCESS'
        }
      }
    }
  }
} catch (error) {
  stage("Notify") {
    script {
      githubNotify context: 'CI Pipeline', status: 'FAILURE'
      if (env.BRANCH_NAME == 'master' || full_ci) {
        githubNotify context: 'Full CI', status: 'FAILURE'
      }
      if (env.BRANCH_NAME == 'master') {
        slackSend message: ":rotating_light: ALARM! Build on ${env.BRANCH_NAME} failed! - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>) :rotating_light:"
      }
    }
    throw error
  }
}
