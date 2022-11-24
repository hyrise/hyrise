import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.TestTransactionConflicts'

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
          stage ("User unknown") {
            script {
              githubNotify context: 'CI Pipeline', status: 'FAILURE', description: 'User is not a collaborator'
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
      def hyriseCI = docker.image('hyrise/hyrise-ci:22.04');
      hyriseCI.pull()

      // LSAN (executed as part of ASAN) requires elevated privileges. Therefore, we had to add --cap-add SYS_PTRACE.
      // Even if the CI run sometimes succeeds without SYS_PTRACE, you should not remove it until you know what you are doing.
      // See also: https://github.com/google/sanitizers/issues/764
      hyriseCI.inside("--cap-add SYS_PTRACE -u 0:0") {
        try {
          stage("Setup") {
            checkout scm

            // During CI runs, the user is different from the owner of the directories, which blocks the execution of git
            // commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be executed if
            // the corresponding directories are added as safe directories.
            sh '''
            git config --global --add safe.directory $WORKSPACE
            # Get the paths of the submodules; for each path, add it as a git safe.directory
            grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
            '''

            sh "./install_dependencies.sh"

            cmake = 'cmake -DCI_BUILD=ON'

            // We don't use unity builds with GCC 9 as it triggers https://github.com/google/googletest/issues/3552
            unity = '-DCMAKE_UNITY_BUILD=ON'
 
            // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
            // support older versions. We test the oldest LLVM version shipped with Ubuntu 22.04 (i.e., LLVM 11) and
            // GCC 9 (oldest version supported by Hyrise). We execute at least debug runs for them.
            // If you want to upgrade compiler versions, please update install_dependencies.sh,  DEPENDENCIES.md, and
            // the documentation (README, Wiki).
            clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
            clang11 = '-DCMAKE_C_COMPILER=clang-11 -DCMAKE_CXX_COMPILER=clang++-11'
            gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'
            gcc9 = '-DCMAKE_C_COMPILER=gcc-9 -DCMAKE_CXX_COMPILER=g++-9'

            debug = '-DCMAKE_BUILD_TYPE=Debug'
            release = '-DCMAKE_BUILD_TYPE=Release'
            relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

            // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the same time, we run into conflicts.
            // Thus, run this one (any one, really) first, so that the autoconf step can finish in peace.

            sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity}  .. && make -j libjemalloc-build"

            // Configure the rest in parallel
            sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}            -DENABLE_CLANG_TIDY=ON .. &\
            mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
            mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}            -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
            mkdir clang-debug-addr-ub-sanitizers && cd clang-debug-addr-ub-sanitizers &&                 ${cmake} ${debug}          ${clang}            -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-release-addr-ub-sanitizers && cd clang-release-addr-ub-sanitizers &&             ${cmake} ${release}        ${clang}   ${unity} -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}            .. &\
            mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}            -DENABLE_THREAD_SANITIZATION=ON .. &\
            mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}              .. &\
            mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}              .. &\
            mkdir clang-11-debug && cd clang-11-debug &&                                                 ${cmake} ${debug}          ${clang11}          .. &\
            mkdir gcc-9-debug && cd gcc-9-debug &&                                                       ${cmake} ${debug}          ${gcc9}             .. &\
            wait"
          }

          parallel clangDebug: {
            stage("clang-debug") {
              sh "cd clang-debug && make all -j \$(( \$(nproc) / 4))"
              sh "./clang-debug/hyriseTest clang-debug"
            }
          }, clang11Debug: {
            stage("clang-11-debug") {
              sh "cd clang-11-debug && make all -j \$(( \$(nproc) / 4))"
              sh "./clang-11-debug/hyriseTest clang-11-debug"
            }
          }, gccDebug: {
            stage("gcc-debug") {
              sh "cd gcc-debug && make all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-debug && ./hyriseTest"
            }
          }, gcc9Debug: {
            stage("gcc-9-debug") {
              sh "cd gcc-9-debug && make all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-9-debug && ./hyriseTest"
            }
          }, lint: {
            stage("Linting") {
              sh '''
                scripts/lint.sh
              '''
            }
          }

          parallel clangRelease: {
            stage("clang-release") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release && make all -j \$(( \$(nproc) / 10))"
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
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate visualization
                sh "./scripts/test/hyriseConsole_test.py gcc-debug"
                sh "./scripts/test/hyriseServer_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-debug"
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate visualization

              } else {
                Utils.markStageSkippedForConditional("debugSystemTests")
              }
            }
          }, clangDebugRunShuffled: {
            stage("clang-debug:test-shuffle") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-debug/run-shuffled"
                sh "./clang-debug/hyriseTest clang-debug/run-shuffled --gtest_repeat=5 --gtest_shuffle"
                sh "./clang-debug/hyriseSystemTest clang-debug/run-shuffled --gtest_repeat=2 --gtest_shuffle"
              } else {
                Utils.markStageSkippedForConditional("clangDebugRunShuffled")
              }
            }
          }, clangDebugUnityODR: {
            stage("clang-debug-unity-odr") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if unity builds work even if everything is batched into a single compilation unit. This helps prevent ODR (one definition rule) issues.
                sh "cd clang-debug-unity-odr && make all -j \$(( \$(nproc) / 10))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugUnityODR")
              }
            }
          }, clangDebugTidy: {
            stage("clang-debug:tidy") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // We do not run tidy checks on the src/test folder, so there is no point in running the expensive clang-tidy for those files
                // As clang-tidy is the slowest step, we allow it to use more parallel jobs.
                sh "cd clang-debug-tidy && make hyrise_impl hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k -j \$(( \$(nproc) / 5))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugTidy")
              }
            }
          }, clangDebugDisablePrecompileHeaders: {
            stage("clang-debug:disable-precompile-headers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if builds work even when precompile headers is turned off. Executing the binaries is unnecessary as the observed errors are missing includes.
                sh "cd clang-debug-disable-precompile-headers && make hyriseTest hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k -j \$(( \$(nproc) / 10))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugDisablePrecompileHeaders")
              }
            }
          }, clangDebugAddrUBSanitizers: {
            stage("clang-debug:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-debug-addr-ub-sanitizers && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 10))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-sanitizers/hyriseTest clang-debug-addr-ub-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-sanitizers/hyriseSystemTest ${tests_excluded_in_sanitizer_builds} clang-debug-addr-ub-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 1"
              } else {
                Utils.markStageSkippedForConditional("clangDebugAddrUBSanitizers")
              }
            }
          }, gccRelease: {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              stage("gcc-release") {
                sh "cd gcc-release && make all -j \$(( \$(nproc) / 10))"
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
          }, clangReleaseAddrUBSanitizers: {
            stage("clang-release:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release-addr-ub-sanitizers && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 10))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseTest clang-release-addr-ub-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseSystemTest ${tests_excluded_in_sanitizer_builds} clang-release-addr-ub-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10"
                sh "cd clang-release-addr-ub-sanitizers && LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
              } else {
                Utils.markStageSkippedForConditional("clangReleaseAddrUBSanitizers")
              }
            }
          }, clangRelWithDebInfoThreadSanitizer: {
            stage("clang-relwithdebinfo:thread-sanitizer") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-relwithdebinfo-thread-sanitizer && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH -j \$(( \$(nproc) / 10))"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseTest clang-relwithdebinfo-thread-sanitizer"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseSystemTest ${tests_excluded_in_sanitizer_builds} clang-relwithdebinfo-thread-sanitizer"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10"
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
              // Runs after the other sanitizers as it depends on gcc-release to be built. With #2402, valgrind now
              // uses the GCC build instead of the clang build as there are issues with valgrind and the debug symbols
              // of clang 14 (https://bugs.kde.org/show_bug.cgi?id=452758).
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-release-memcheck-test"
                // If this shows a leak, try --leak-check=full, which is slower but more precise
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./gcc-release/hyriseTest clang-release-memcheck-test --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./gcc-release/hyriseBenchmarkTPCH -s .01 -r 1 --scheduler --cores 10"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./gcc-release/hyriseBenchmarkTPCC -s 1 --scheduler --cores 10"
              } else {
                Utils.markStageSkippedForConditional("memcheckReleaseTest")
              }
            }
          }, tpchVerification: {
            stage("tpchVerification") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "./clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 1 -s 1 --verify"
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
          }
        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
        }
      }
    }
  }

  parallel clangDebugMacX64: {
    node('mac') {
      stage("clangDebugMacX64") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm

            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"

            sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${debug} ${unity} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@14/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@14/bin/clang++ .."
            sh "cd clang-debug && make -j8"
            sh "./clang-debug/hyriseTest"
            sh "./clang-debug/hyriseSystemTest --gtest_filter=-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangDebugMacX64")
        }
      }
    }
  }, clangReleaseMacArm: {
    // For this to work, we installed a native non-standard JDK (zulu) via brew. See #2339 for more details.
    node('mac-arm') {
      stage("clangReleaseMacArm") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm          
            
            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"
            
            // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
            sh "mkdir clang-release && cd clang-release && cmake ${release} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang++ .."
            sh "cd clang-release && make -j8"

            // Check whether arm64 binaries are built to ensure that we are not accidentally running rosetta that
            // executes x86 binaries on arm.
            sh "file ./clang-release/hyriseTest | grep arm64"

            sh "./clang-release/hyriseTest"
            sh "./clang-release/hyriseSystemTest --gtest_filter=-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangReleaseMacArm")
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
