node {

  def oppossumCI = docker.image('hyrise/opossum-ci:17.10');
  oppossumCI.pull()
  // create ccache volume on host using:
  // mkdir /mnt/ccache; mount -t tmpfs -o size=10G none /mnt/ccache

  oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CACHE_MAXSIZE=10GB\" -e \"CCACHE_SLOPPINESS=file_macro\"") {

    try {
      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "mkdir clang-debug && cd clang-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 .. &\
        mkdir clang-debug-sanitizers && cd clang-debug-sanitizers && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 -DENABLE_SANITIZATION=ON .. &\
        mkdir clang-release-sanitizers && cd clang-release-sanitizers && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 -DENABLE_SANITIZATION=ON .. &\
        mkdir clang-release && cd clang-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 .. &\
        mkdir clang-release-sanitizers-no-numa && cd clang-release-sanitizers-no-numa && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 -DENABLE_SANITIZATION=ON -DDISABLE_NUMA_SUPPORT=ON .. &\
        mkdir gcc-release && cd gcc-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-debug-coverage && cd gcc-debug-coverage && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DENABLE_COVERAGE=ON .. &\
        wait"
      }

      stage("Linting") {
        sh '''
          scripts/lint.sh pre
        '''
      }

      parallel clangRelease: {
        stage("clang-release") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./clang-release/hyriseTest"
        }
      }, clangDebugBuildOnly: {
        stage("clang-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
        }
      }, moreLint: {
        stage("Stricter Linting") {
          script {
            lintFails = sh script: "./scripts/lint.sh post || true", returnStdout: true
            if (lintFails?.trim()) {
              echo lintFails
              writeFile file: "post_lint.txt", text: lintFails
              archive "post_lint.txt"
              githubNotify context: 'Strict Lint', status: 'ERROR', description: "Click Details", targetUrl: "${env.BUILD_URL}/artifact/post_lint.txt"
            } else {
              githubNotify context: 'Strict Lint', status: 'SUCCESS'
            }
          }
        }
      }

      parallel clangDebugRun: {
        stage("clang-debug:test") {
          sh "./clang-debug/hyriseTest"
        }
      }, clangDebugRunShuffled: {
        stage("clang-debug:test-shuffle") {
          sh "./clang-debug/hyriseTest --gtest_repeat=5 --gtest_shuffle"
        }
      }, clangDebugSanitizers: {
        stage("clang-debug:sanitizers") {
        sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug-sanitizers && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-debug-sanitizers/hyriseTest"
        }
      }, gccRelease: {
        stage("gcc-release") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./gcc-release/hyriseTest"
        }
      }, systemTest: {
        stage("System Test") {
            sh "./scripts/run_system_test.sh clang-release"
        }
      }, clangReleaseSanitizers: {
        stage("clang-release:sanitizers") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-sanitizers && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release-sanitizers/hyriseTest"
        }
      }, clangReleaseSanitizersNoNuma: {
        stage("clang-release:sanitizers w/o NUMA") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-sanitizers-no-numa && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release-sanitizers-no-numa/hyriseTest"
        }
      }, gccDebugCoverage: {
        stage("gcc-debug-coverage") {
          sh "export CCACHE_BASEDIR=`pwd`; ./scripts/coverage.sh gcc-debug-coverage true"
          archive 'coverage_badge.svg'
          archive 'coverage_percent.txt'
          archive 'coverage.xml'
          archive 'coverage_diff.html'
          publishHTML (target: [
            allowMissing: false,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: 'coverage',
            reportFiles: 'index.html',
            reportName: "RCov Report"
          ])
          script {
            coverageChange = sh script: "./scripts/compare_coverage.sh", returnStdout: true
            githubNotify context: 'Coverage', description: "$coverageChange", status: 'SUCCESS', targetUrl: "${env.BUILD_URL}/RCov_Report/index.html"
            githubNotify context: 'Coverage Diff', description: "Click Details for diff", status: 'SUCCESS', targetUrl: "${env.BUILD_URL}/artifact/coverage_diff.html"
          }
        }
      }, memcheck: {
        stage("valgrind-memcheck") {
          sh "valgrind --tool=memcheck --error-exitcode=1 --leak-check=full --gen-suppressions=all --suppressions=.valgrind-ignore.txt ./clang-release/hyriseTest --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
        }
      }

      stage("Cleanup") {
        // Clean up workspace.
        githubNotify context: 'CI Pipeline', status: 'SUCCESS'
        step([$class: 'WsCleanup'])
      }
    } catch (error) {
      stage "Cleanup after fail"
      githubNotify context: 'CI Pipeline', status: 'FAILURE'
      throw error
    } finally {
      
      sh "ls -A1 | xargs rm -rf"
      deleteDir()
    }

  }

}

