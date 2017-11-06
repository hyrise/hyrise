node {

  def oppossumCI = docker.image('hyrise/opossum-ci:17.10');
  oppossumCI.pull()
  // create ccache volume on host using:
  // mkdir /mnt/ccache; mount -t tmpfs -o size=10G none /mnt/ccache

  oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CACHE_MAXSIZE=10GB\" -e \"CCACHE_SLOPPINESS=file_macro\"") {

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
        sh "mkdir clang-debug && cd clang-debug && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 .. &\
        mkdir clang-release && cd clang-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 .. &\
        mkdir clang-release-no-numa && cd clang-release-no-numa && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 -DDISABLE_NUMA_SUPPORT=On .. &\
        mkdir gcc-release && cd gcc-release && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-debug-coverage && cd gcc-debug-coverage && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
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
          sh "./clang-release/hyriseTest"
        }
      }, clangDebugBuildOnly: {
        stage("clang-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
        }
      }

      parallel clangDebugRun: {
        stage("clang-debug:test") {
          sh "./clang-debug/hyriseTest"
        }
      }, clangDebugSanitizers: {
        stage("clang-debug:sanitizers") {
        sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make hyriseSanitizers -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-debug/hyriseSanitizers"
        }
      }, gccRelease: {
        stage("gcc-release") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./gcc-release/hyriseTest"
        }
      }, tpcc: {
        stage("TPCC Test") {
            sh "./scripts/test_tpcc.sh clang-release"
        }
      }, clangReleaseSanitizers: {
        stage("clang-release:sanitizers") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make hyriseSanitizers -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release/hyriseSanitizers"
        }
      }, clangReleaseSanitizersNoNuma: {
        stage("clang-release:sanitizers w/o NUMA") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-no-numa && make hyriseSanitizers -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "LSAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release-no-numa/hyriseSanitizers"
        }
      }, gccDebugCoverage: {
        stage("gcc-debug-coverage") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-debug-coverage && make hyriseCoverage -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./scripts/coverage.sh gcc-debug-coverage true"
          archive 'coverage_badge.svg'
          archive 'coverage_percent.txt'
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
          }
        }
      }, memcheck: {
        stage("valgrind-memcheck") {
          sh "valgrind --tool=memcheck --error-exitcode=1 --leak-check=full --gen-suppressions=all --suppressions=.valgrind-ignore.txt ./clang-release/hyriseTest --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
        }
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
