node {

  docker.image('hyrise/opossum-ci:16.10').inside("-u 0:0") {

    try {
      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "git submodule update --init"
      }

      stage("Linting") {
        sh '''
          find src -iname *.cpp -o -iname *.hpp | while read line;
            do
                if ! python2.7 cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 $line >/dev/null 2>/dev/null
                then
                    echo "ERROR: Linting error occured. Execute \"premake4 lint\" for details!"
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
          sh "premake4 --compiler=gcc"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release test"
        }
        stage("gc Debug") {
          sh "make clean"
          sh "premake4 --compiler=gcc"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug test"
        }
      }

      stage("Test clang") {
        stage("clang Release") {
          sh "make clean"
          sh "premake4 --compiler=clang"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release test"
        }
        stage("clang Debug") {
          sh "make clean"
          sh "premake4 --compiler=clang"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug test"
        }
      }

      stage("ASAN") {
        stage("asan Release") {
          sh "make clean"
          sh "premake4 --compiler=clang"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release asan"
        }
        stage("asan Debug") {
          sh "make clean"
          sh "premake4 --compiler=clang"
          sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug asan"
        }
      }

      stage("Coverage") {
        sh "make clean"
        sh "premake4"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) coverage"
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
    }
  } catch (error) {
    stage "Cleanup after fail"
    throw error
  } finally {
    deleteDir()
  }

}