node {

  def oppossumCI = docker.image('hyrise/opossum-ci:17.04');
  oppossumCI.pull()
  oppossumCI.inside("-u 0:0") {

    try {

      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "git submodule update --init --recursive"
        sh "mkdir clang-debug && cd clang-debug && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
        sh "mkdir clang-release && cd clang-release && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
        sh "mkdir gcc-debug && cd gcc-debug && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .."
        sh "mkdir gcc-release && cd gcc-release && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .."
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

      stage("Build") {
        stage("Build gcc") {
          stage("Build gcc Release") {
            sh "cd gcc-release && make opossumTest opossumAsan -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
          stage("Build gcc Debug") {
            sh "cd gcc-debug && make opossumTest opossumAsan -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
        }

        stage("Build clang") {
          stage("Build clang release") {
            sh "cd clang-release && make opossumTest opossumAsan -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
          stage("Build clang debug") {
            sh "cd clang-debug && make opossumTest opossumAsan -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
        }
      }

      stage("Test") {
        stage("Test gcc") {
          stage("Test gcc Release") {
            sh "./gcc-release/opossumTest"
          }
          stage("Test gcc Debug") {
            sh "./gcc-debug/opossumTest"
          }
        }

        stage("Test clang") {
          stage("Test clang Release") {
            sh "./clang-release/opossumTest"
          }
          stage("Test clang Debug") {
            sh "./clang-debug/opossumTest"
          }
        }
      }

      stage("ASAN") {
        stage("asan Release") {
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-release/opossumAsan"
        }
        stage("asan Debug") {
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-debug/opossumAsan"
        }
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
          sh "cd clang-debug && make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTestTPCC tpccTableGenerator"
          sh "./scripts/test_tpcc.sh clang-debug"
      }

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
