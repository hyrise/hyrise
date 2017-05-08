node {

  docker.image('hyrise/opossum-ci:16.10').inside("-u 0:0") {

    try {

      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "git submodule update --init"
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

      stage("Test gcc") {
        stage("gcc Release") {
          sh "cd gcc-release"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTest"
          sh "./opossumTest"
        }
        stage("gcc Debug") {
          sh "cd gcc-debug"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTest"
          sh "./opossumTest"
        }
      }

      stage("Test clang") {
        stage("clang Release") {
          sh "cd clang-release"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTest"
          sh "./opossumTest"
        }
        stage("clang Debug") {
          sh "cd clang-debug"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumTest"
          sh "./opossumTest"
        }
      }

      stage("ASAN") {
        stage("asan Release") {
          sh "cd clang-release"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumAsan"
          sh "./opossumAsan"
        }
        stage("asan Debug") {
          sh "cd clang-debug"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumAsan"
          sh "./opossumAsan"
        }
      }

      stage("Coverage") {
        sh "cd clang-debug"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) opossumCoverage"
        sh "./opossumCoverage"
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
