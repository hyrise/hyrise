node {

  def oppossumCI = docker.image('hyrise/opossum-ci:17.04');
  oppossumCI.pull()
  // create ccache volume on host using:
  // mkdir /mnt/ccache; mount -t tmpfs -o size=10G none /mnt/ccache

  oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CACHE_MAXSIZE=10GB\"") {

    try {
      stage("Setup") {
        checkout([
             $class: 'GitSCM',
             branches: scm.branches,
             doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
             extensions: scm.extensions + [$class: 'CloneOption', noTags: true, reference: '', shallow: true, honorRefspec: true],
             // Set the remote by hand so that we can only check out the branch that we want:
             userRemoteConfigs: [[refspec: "+refs/heads/" + scm.branches[0] + ":refs/remotes/origin/" + scm.branches[0],
                                  url : scm.userRemoteConfigs.get(0).getUrl(),
                                  credentialsId: scm.userRemoteConfigs.get(0).getCredentialsId()]],
        ])
        sh "./install.sh"
        sh "mkdir clang-debug && cd clang-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. &\
        mkdir clang-release && cd clang-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .. &\
        mkdir gcc-debug && cd gcc-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-release && cd gcc-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-release-coverage && cd gcc-release-coverage && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
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

      parallel clangRelease: {
        stage("clang-release") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./clang-release/opossumTest"
        }
      }, clangDebug: {
        stage("clang-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
        }
      }

      parallel clangDebug: {
        stage("clang-debug:test") {
          sh "./clang-debug/opossumTest"
        }
      }, clangDebugAsan: {
        stage("clang-debug:asan") {
        sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make opossumAsan -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-debug/opossumAsan"
        }
      }, gccDebug: {
        stage("gcc-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./gcc-debug/opossumTest"
        }
      }, gccRelease: {
        stage("gcc-release") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./gcc-release/opossumTest"
        }
      }, tpcc: {
        stage("TPCC Test") {
            sh "./scripts/test_tpcc.sh clang-release"
        }
      }, asanRelease: {
        stage("clang-release:asan") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make opossumAsan -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=asan-ignore.txt ./clang-release/opossumAsan"
        }
      }, coverage: {
        stage("Coverage") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release-coverage && make opossumCoverage -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./scripts/coverage.sh gcc-release-coverage true"
          publishHTML (target: [
            allowMissing: false,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: 'coverage',
            reportFiles: 'index.html',
            reportName: "RCov Report"
          ])
          archiveArtifacts artifacts: 'coverage_badge.svg', fingerprint: true
        }
      },

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
