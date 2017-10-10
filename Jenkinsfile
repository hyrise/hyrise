node {

  def oppossumCI = docker.image('hyrise/opossum-ci:17.04');
  oppossumCI.pull()
  // create ccache volume using:
  // mkdir /mnt/ccache; mount -t tmpfs -o size=10G none /mnt/ccache
  oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CACHE_MAXSIZE=10GB\"") {

    try {
//      failFast true

      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "sudo apt-get install ccache"
        sh "mkdir clang-debug && cd clang-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. &\
        mkdir clang-release && cd clang-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. &\
        mkdir gcc-debug && cd gcc-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-release && cd gcc-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        wait"
      }

      stage("Linting") {
        sh '''
          scripts/lint.sh

          if [ $? != 0 ]; then
            echo "ERROR: Linting error occured. Execute \"scripts/lint.sh\" for details!"
            exit 1
          fi
        '''
      }

      stage("Build and Test") {
        stage("clang-debug") {
          sh "cd clang-debug && make all opossumCoverage opossumAsan -j \$(( $(cat /proc/cpuinfo | grep processor | wc -l) / 5))"
          sh "./clang-debug/opossumTest"
        }
//        parallel {
          stage("clang-release") {
            sh "cd clang-release && make all opossumAsan -j \$(( $(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./clang-release/opossumTest"
          }
          stage("gcc-debug") {
            sh "cd gcc-debug && make all -j \$(( $(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./gcc-debug/opossumTest"
          }
          stage("gcc-release") {
            sh "cd gcc-release && make all -j \$(( $(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./gcc-release/opossumTest"
          }
//        }
      }

//      parallel {
        stage("asan Release") {
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-release/opossumAsan"
        }
        stage("asan Debug") {
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-debug/opossumAsan"
        }

        stage("Coverage") {
          sh "./scripts/coverage.sh clang-debug"
          publishHTML (target: [
            allowMissing: false,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: 'coverage',
            reportFiles: 'index.html',
            reportName: "RCov Report"
          ])
        }

        stage("TPCC Test") {
            sh "cd clang-release && make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTestTPCC tpccTableGenerator"
            sh "./scripts/test_tpcc.sh clang-release"
        }
//      }

      stage("Cleanup") {
        // Clean up workspace.
        step([$class: 'WsCleanup'])
      }

    } catch (error) {
      stage "Cleanup after fail"
      throw error
    } finally {
      sh "ls -A1 | xargs rm -rf"
      deleteDir()
    }

  }

}
