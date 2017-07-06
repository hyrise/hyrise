node {

  def oppossumCI = docker.image('hyrise/opossum-ci:16.10');
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
          find src -iname *.cpp -o -iname *.hpp | while read line;
            do
                if ! python2.7 cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 $line >/dev/null 2>/dev/null
                then
                    echo "ERROR: Linting error occured. Execute \"tools/lint.sh\" for details!"
                    exit 1
                fi
            done

            if [ $? != 0 ]
            then
                exit 1
            fi
        '''
      }

      stage("Build") {
        stage("Build gcc") {
          stage("Build gcc Release") {
            sh "cd gcc-release && make -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
          stage("Build gcc Debug") {
            sh "cd gcc-debug && make -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
        }

        stage("Build clang") {
          stage("Build clang release") {
            sh "cd clang-release && make -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
          }
          stage("Build clang debug") {
            sh "cd clang-debug && make -j \$(cat /proc/cpuinfo | grep processor | wc -l)"
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
          sh "./clang-release/opossumAsan"
        }
        stage("asan Debug") {
          sh "./clang-debug/opossumAsan"
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

      stage("Cleanup") {
        // Clean up workspace
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
