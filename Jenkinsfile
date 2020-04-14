import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TpcdsTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor'

try {
  node('master') {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team)
      if (env.CHANGE_ID) {
        try {
          withCredentials([usernamePassword(credentialsId: '5fe8ede9-bbdb-4803-a307-6924d4b4d9b5', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
            env.PR_CREATED_BY = pullRequest.createdBy
            sh '''
              curl -s -I -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/hyrise/hyrise/collaborators/${PR_CREATED_BY} | head -n 1 | grep "HTTP/1.1 204 No Content"
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
    def oppossumCI = docker.image('hyrise/opossum-ci:19.10');
    oppossumCI.pull()
    // create ccache volume on host using:
    // mkdir /mnt/ccache; mount -t tmpfs -o size=200G none /mnt/ccache
    // or add it to /etc/fstab:
    // tmpfs  /mnt/ccache tmpfs defaults,size=200G  0 0

    oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_MAXSIZE=200GB\" -e \"CCACHE_SLOPPINESS=file_macro,pch_defines,time_macros\" -e\"CCACHE_DEPEND=1\" -e\"CCACHE_NOHASHDIR=1\" -e\"CCACHE_DEBUG=1\" --privileged=true") {
      try {
        stage("Setup") {
          checkout scm
          sh "./install_dependencies.sh"

          cmake = 'cmake -DCI_BUILD=ON'
          ccache = '-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache'
          unity = '-DCMAKE_UNITY_BUILD=ON'

          clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
          gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'

          debug = '-DCMAKE_BUILD_TYPE=Debug'
          release = '-DCMAKE_BUILD_TYPE=Release'
          relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

          // GCC produces non-deterministics files when precompiled headers are built: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92717
          // Disable ASLR for the gcc build process (not for the resulting executable) to stop this from happening:
          disable_aslr = 'setarch x86_64 -R'

          // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the same time, we run into conflicts.
          // Thus, run this one (any one, really) first, so that the autoconf step can finish in peace.

          sh "mkdir clang-debug && cd clang-debug &&                                                  ${cmake} ${debug}                    ${clang} ${unity} .. && make -j libjemalloc-build"

          // Configure the rest in parallel
          // clang + precompiled headers + ccache is broken: https://gitlab.kitware.com/cmake/cmake/issues/19923
          // Some configurations don't use unity builds as this would break the ccache use across different PRs.
          // For clang-tidy, ccache does not have an effect, so we use unity there.
          sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                        ${cmake}           ${debug}          ${clang} ${unity} -DENABLE_CLANG_TIDY=ON .. &\
          mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                  ${cmake}           ${debug}          ${clang} ${unity} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
          mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers &&    ${cmake}           ${debug}          ${clang}          -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
          mkdir clang-debug-addr-ub-sanitizers && cd clang-debug-addr-ub-sanitizers &&                ${cmake}           ${debug}          ${clang}          -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
          mkdir clang-release-addr-ub-sanitizers && cd clang-release-addr-ub-sanitizers &&            ${cmake}           ${release}        ${clang}          -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
          mkdir clang-release && cd clang-release &&                                                  ${cmake}           ${release}        ${clang}          .. &\
          mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&  ${cmake}           ${relwithdebinfo} ${clang}          -DENABLE_THREAD_SANITIZATION=ON .. &\
          mkdir gcc-debug && cd gcc-debug &&                                                          ${cmake} ${ccache} ${debug}          ${gcc}            .. &\
          mkdir gcc-release && cd gcc-release &&                                                      ${cmake} ${ccache} ${release}        ${gcc}            .. &\
          wait"
        }

        parallel clangDebug: {
          stage("clang-debug") {
            sh "cd clang-debug && make all -j \$(( \$(nproc) / 4))"
            sh "./clang-debug/hyriseTest clang-debug"
          }
        }, gccDebug: {
          stage("gcc-debug") {
            sh "export CCACHE_BASEDIR=`pwd`; cd gcc-debug && ${disable_aslr} make all -j \$(( \$(nproc) / 4)) && ../scripts/analyze_ccache_usage.py"
            sh "cd gcc-debug && ./hyriseTest"
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
              sh "cd clang-release && make all -j \$(( \$(nproc) / 6))"
              sh "./clang-release/hyriseTest clang-release"
              sh "./clang-release/hyriseSystemTest clang-release"
              sh "./scripts/test/hyriseConsole_test.py clang-release"
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-release"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
              sh "./scripts/test/hyriseBenchmarkTPCC_test.py clang-release"
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
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-debug"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
              sh "cd clang-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
              sh "./scripts/test/hyriseConsole_test.py gcc-debug"
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-debug"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-debug"
              sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization

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
              sh "cd clang-debug-unity-odr && make all -j \$(( \$(nproc) / 3))"
            } else {
              Utils.markStageSkippedForConditional("clangDebugUnityODR")
            }
          }
        }, clangDebugTidy: {
          stage("clang-debug:tidy") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              // We do not run tidy checks on the src/test folder, so there is no point in running the expensive clang-tidy for those files
              sh "cd clang-debug-tidy && make hyrise_impl hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k -j \$(( \$(nproc) / 6))"
            } else {
              Utils.markStageSkippedForConditional("clangDebugTidy")
            }
          }
        }, clangDebugDisablePrecompileHeaders: {
          stage("clang-debug:disable-precompile-headers") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              // Check if builds work even when precompile headers is turned off. Executing the binaries is unnecessary as the observed errors are missing includes.
              sh "cd clang-debug-disable-precompile-headers && make hyriseTest hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k -j \$(( \$(nproc) / 6))"
            } else {
              Utils.markStageSkippedForConditional("clangDebugDisablePrecompileHeaders")
            }
          }
        }, clangDebugAddrUBSanitizers: {
          stage("clang-debug:addr-ub-sanitizers") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "cd clang-debug-addr-ub-sanitizers && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 6))"
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
              sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && ${disable_aslr} make all -j \$(( \$(nproc) / 6)) && ../scripts/analyze_ccache_usage.py"
              sh "./gcc-release/hyriseTest gcc-release"
              sh "./gcc-release/hyriseSystemTest gcc-release"
              sh "./scripts/test/hyriseConsole_test.py gcc-release"
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-release"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-release"
              sh "./scripts/test/hyriseBenchmarkTPCC_test.py gcc-release"
              sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
            }
          } else {
              Utils.markStageSkippedForConditional("gccRelease")
          }
        }, clangReleaseAddrUBSanitizers: {
          stage("clang-release:addr-ub-sanitizers") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "cd clang-release-addr-ub-sanitizers && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 6))"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseTest clang-release-addr-ub-sanitizers"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseSystemTest ${tests_excluded_in_sanitizer_builds} clang-release-addr-ub-sanitizers"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./scripts/test/hyriseBenchmarkTPCC_test.py clang-release-addr-ub-sanitizers"
            } else {
              Utils.markStageSkippedForConditional("clangReleaseAddrUBSanitizers")
            }
          }
        }, clangRelWithDebInfoThreadSanitizer: {
          stage("clang-relwithdebinfo:thread-sanitizer") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "cd clang-relwithdebinfo-thread-sanitizer && make hyriseTest hyriseSystemTest hyriseBenchmarkTPCH -j \$(( \$(nproc) / 6))"
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
            // Runs separately as it depends on clang-release to be built
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "mkdir ./clang-release-memcheck-test"
              // If this shows a leak, try --leak-check=full, which is slower but more precise
              sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseTest clang-release-memcheck-test --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
              sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCH -s .01 -r 1 --scheduler --cores 10"
              sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCC -s 1 --scheduler --cores 10"
            } else {
              Utils.markStageSkippedForConditional("memcheckReleaseTest")
            }
          }
        }, tpchQueryPlansAndVerification: {
          stage("tpchQueryPlansAndVerification") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "mkdir -p query_plans/tpch; cd query_plans/tpch; ln -s ../../resources; ../../clang-release/hyriseBenchmarkTPCH -r 1 --visualize --verify"
              archiveArtifacts artifacts: 'query_plans/tpch/*.svg'
            } else {
              Utils.markStageSkippedForConditional("tpchQueryPlansAndVerification")
            }
          }
        }, tpcdsQueryPlansAndVerification: {
          stage("tpcdsQueryPlansAndVerification") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "mkdir -p query_plans/tpcds; cd query_plans/tpcds; ln -s ../../resources; ../../clang-release/hyriseBenchmarkTPCDS -r 1 --visualize --verify"
              archiveArtifacts artifacts: 'query_plans/tpcds/*.svg'
            } else {
              Utils.markStageSkippedForConditional("tpcdsQueryPlansAndVerification")
            }
          }
        }
      } finally {
        sh "ls -A1 | xargs rm -rf"
        deleteDir()
      }
    }
  }

  // I have not found a nice way to run this in parallel with the steps above, as those are in a `docker.inside` block and this is not.
  node('mac') {
    stage("clangDebugMac") {
      if (env.BRANCH_NAME == 'master' || full_ci) {
        try {
          checkout scm

          // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
          sh "git submodule update --init --recursive --jobs 4"

          sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${unity} ${debug} -DCMAKE_C_COMPILER=/usr/local/Cellar/llvm/9.0.0/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/Cellar/llvm/9.0.0/bin/clang++ .."
          sh "cd clang-debug && PATH=/usr/local/bin:$PATH make -j libjemalloc-build"
          sh "cd clang-debug && make -j8"
          sh "./clang-debug/hyriseTest"
          sh "./clang-debug/hyriseSystemTest --gtest_filter=-TPCCTest*:TpcdsTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4"
          sh "./scripts/test/hyriseConsole_test.py clang-debug"
          sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
        } finally {
          sh "ls -A1 | xargs rm -rf"
        }
      } else {
        Utils.markStageSkippedForConditional("clangDebugMac")
      }
    }
  }

  node ('master') {
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
