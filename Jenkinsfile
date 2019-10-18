import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
exclude_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings.*'

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
    def oppossumCI = docker.image('hyrise/opossum-ci:19.04');
    oppossumCI.pull()
    // create ccache volume on host using:
    // mkdir /mnt/ccache; mount -t tmpfs -o size=50G none /mnt/ccache
    // or add it to /etc/fstab:
    // tmpfs  /mnt/ccache tmpfs defaults,size=51201M  0 0

    oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CCACHE_MAXSIZE=50GB\" -e \"CCACHE_SLOPPINESS=file_macro\" --privileged=true") {
      try {
        stage("Setup") {
          checkout scm
          sh "./install.sh"

          // Run cmake once in isolation and build jemalloc to avoid race conditions with autoconf (#1413)
          sh "mkdir clang-debug && cd clang-debug && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. && make -j libjemalloc-build"

          // Configure the rest in parallel
          sh "mkdir clang-debug-tidy && cd clang-debug-tidy && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_CLANG_TIDY=ON .. &\
          mkdir clang-debug-addr-ub-sanitizers && cd clang-debug-addr-ub-sanitizers && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
          mkdir clang-release-addr-ub-sanitizers && cd clang-release-addr-ub-sanitizers && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
          mkdir clang-release && cd clang-release && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. &\
          mkdir clang-release-addr-ub-sanitizers-no-numa && cd clang-release-addr-ub-sanitizers-no-numa && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_ADDR_UB_SANITIZATION=ON -DENABLE_NUMA_SUPPORT=OFF .. &\
          mkdir clang-release-thread-sanitizer && cd clang-release-thread-sanitizer && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_THREAD_SANITIZATION=ON .. &\
          mkdir clang-release-thread-sanitizer-no-numa && cd clang-release-thread-sanitizer-no-numa && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DENABLE_THREAD_SANITIZATION=ON -DENABLE_NUMA_SUPPORT=OFF .. &\
          mkdir gcc-debug && cd gcc-debug && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
          mkdir gcc-release && cd gcc-release && cmake -DCI_BUILD=ON -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
          wait"
        }

        parallel clangDebug: {
          stage("clang-debug") {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./clang-debug/hyriseTest clang-debug"
          }
        }, gccDebug: {
          stage("gcc-debug") {
            sh "export CCACHE_BASEDIR=`pwd`; cd gcc-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
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
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "./clang-release/hyriseTest clang-release"
              sh "./clang-release/hyriseSystemTest clang-release"
              sh "./scripts/test/hyriseConsole_test.py clang-release"
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-release"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
              sh "./clang-release/hyriseBenchmarkTPCDS --verify -r 1"
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
        }, clangDebugTidy: {
          stage("clang-debug:tidy") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              // We do not run tidy checks on the src/test folder, so there is no point in running the expensive clang-tidy for those files
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug-tidy && make hyrise hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -l -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            } else {
              Utils.markStageSkippedForConditional("clangDebugTidy")
            }
          }
        }, clangDebugAddrUBSanitizers: {
          stage("clang-debug:addr-ub-sanitizers") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug-addr-ub-sanitizers && make hyriseTest hyriseSystemTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-sanitizers/hyriseTest clang-debug-addr-ub-sanitizers"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-sanitizers/hyriseSystemTest ${exclude_in_sanitizer_builds} clang-debug-addr-ub-sanitizers"
            } else {
              Utils.markStageSkippedForConditional("clangDebugAddrUBSanitizers")
            }
          }
        }, gccRelease: {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            stage("gcc-release") {
              sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "./gcc-release/hyriseTest gcc-release"
              sh "./gcc-release/hyriseSystemTest gcc-release"
              sh "./scripts/test/hyriseConsole_test.py gcc-release"
              sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-release"
              sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-release"
              sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
            }
          } else {
              Utils.markStageSkippedForConditional("gccRelease")
          }
        }, clangReleaseAddrUBSanitizers: {
          stage("clang-release:addr-ub-sanitizers") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-addr-ub-sanitizers && make hyriseTest hyriseSystemTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseTest clang-release-addr-ub-sanitizers"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers/hyriseSystemTest ${exclude_in_sanitizer_builds} clang-release-addr-ub-sanitizers"
            } else {
              Utils.markStageSkippedForConditional("clangReleaseAddrUBSanitizers")
            }
          }
        }, clangReleaseAddrUBSanitizersNoNuma: {
          stage("clang-release:addr-ub-sanitizers w/o NUMA") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-addr-ub-sanitizers-no-numa && make hyriseTest hyriseSystemTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers-no-numa/hyriseTest clang-release-addr-ub-sanitizers-no-numa"
              sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-sanitizers-no-numa/hyriseSystemTest ${exclude_in_sanitizer_builds} clang-release-addr-ub-sanitizers-no-numa"
            } else {
              Utils.markStageSkippedForConditional("clangReleaseAddrUBSanitizersNoNuma")
            }
          }
        }, clangReleaseThreadSanitizer: {
          stage("clang-release:thread-sanitizer") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-thread-sanitizer && make hyriseTest hyriseSystemTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "TSAN_OPTIONS=suppressions=resources/.tsan-ignore.txt ./clang-release-thread-sanitizer/hyriseTest clang-release-thread-sanitizer"
              sh "TSAN_OPTIONS=suppressions=resources/.tsan-ignore.txt ./clang-release-thread-sanitizer/hyriseSystemTest ${exclude_in_sanitizer_builds} clang-release-thread-sanitizer"
            } else {
              Utils.markStageSkippedForConditional("clangReleaseThreadSanitizer")
            }
          }
        }, clangReleaseThreadSanitizerNoNuma: {
          stage("clang-release:thread-sanitizer w/o NUMA") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-thread-sanitizer-no-numa && make hyriseTest hyriseSystemTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
              sh "TSAN_OPTIONS=suppressions=resources/.tsan-ignore.txt ./clang-release-thread-sanitizer-no-numa/hyriseTest clang-release-thread-sanitizer-no-numa"
              sh "TSAN_OPTIONS=suppressions=resources/.tsan-ignore.txt ./clang-release-thread-sanitizer-no-numa/hyriseSystemTest ${exclude_in_sanitizer_builds} clang-release-thread-sanitizer-no-numa"
            } else {
              Utils.markStageSkippedForConditional("clangReleaseThreadSanitizerNoNuma")
            }
          }
        }, clangDebugCoverage: {
          stage("clang-debug-coverage") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "export CCACHE_BASEDIR=`pwd`; ./scripts/coverage.sh --generate_badge=true --launcher=ccache"
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
            } else {
              Utils.markStageSkippedForConditional("memcheckReleaseTest")
            }
          }
        }, tpchQueryPlans: {
          stage("tpchQueryPlans") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "mkdir -p query_plans/tpch; cd query_plans/tpch; ../../clang-release/hyriseBenchmarkTPCH -r 1 --visualize"
              archiveArtifacts artifacts: 'query_plans/tpch/*.svg'
            } else {
              Utils.markStageSkippedForConditional("tpchQueryPlans")
            }
          }
        }, tpcdsQueryPlans: {
          stage("tpcdsQueryPlans") {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              sh "mkdir -p query_plans/tpcds; cd query_plans/tpcds; ln -s ../../resources; ../../clang-release/hyriseBenchmarkTPCDS -r 1 --visualize"
              archiveArtifacts artifacts: 'query_plans/tpcds/*.svg'
            } else {
              Utils.markStageSkippedForConditional("tpcdsQueryPlans")
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

          // We do not use install.sh here as there is no way to run OS X in a Docker container
          sh "git submodule update --init --recursive --jobs 4"

          sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake -DCMAKE_C_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/local/Cellar/llvm/8.0.0/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/Cellar/llvm/8.0.0/bin/clang++ .."
          sh "cd clang-debug && PATH=/usr/local/bin:$PATH make -j libjemalloc-build"
          sh "cd clang-debug && CCACHE_CPP2=yes CCACHE_SLOPPINESS=file_macro CCACHE_BASEDIR=`pwd` make -j8"
          sh "./clang-debug/hyriseTest"
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
