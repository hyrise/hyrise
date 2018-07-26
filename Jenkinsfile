import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

node {
  stage ("Start") {
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

  def oppossumCI = docker.image('hyrise/opossum-ci:18.04');
  oppossumCI.pull()
  // create ccache volume on host using:
  // mkdir /mnt/ccache; mount -t tmpfs -o size=10G none /mnt/ccache

  oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_CPP2=yes\" -e \"CCACHE_MAXSIZE=10GB\" -e \"CCACHE_SLOPPINESS=file_macro\"") {
    try {
      stage("Setup") {
        checkout scm
        sh "./install.sh"
        sh "mkdir clang-debug && cd clang-debug && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 .. &\
        mkdir clang-debug-tidy && cd clang-debug-tidy && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 -DENABLE_CLANG_TIDY=ON .. &\
        mkdir clang-debug-sanitizers && cd clang-debug-sanitizers && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 -DENABLE_SANITIZATION=ON .. &\
        mkdir clang-release-sanitizers && cd clang-release-sanitizers && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 -DENABLE_SANITIZATION=ON .. &\
        mkdir clang-release && cd clang-release && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 .. &\
        mkdir clang-release-sanitizers-no-numa && cd clang-release-sanitizers-no-numa && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-6.0 -DCMAKE_CXX_COMPILER=clang++-6.0 -DENABLE_SANITIZATION=ON -DENABLE_NUMA_SUPPORT=OFF .. &\
        mkdir gcc-debug && cd gcc-debug && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        mkdir gcc-release && cd gcc-release && cmake -DCI_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ .. &\
        wait"
        full_ci = sh(script: "./scripts/current_branch_has_pull_request_label.py FullCI", returnStdout: true).trim() == "true"
      }

      parallel clangRelease: {
        stage("clang-release") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./clang-release/hyriseTest clang-release"
          } else {
            Utils.markStageSkippedForConditional("clangRelease")
          }
        }
      }, clangDebugBuildOnly: {
        stage("clang-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
        }
      }, lint: {
        stage("Linting") {
          sh '''
            scripts/lint.sh
          '''
        }
      }

      parallel clangDebugRun: {
        stage("clang-debug:test") {
          sh "./clang-debug/hyriseTest clang-debug"
        }
      }, clangDebugRunShuffled: {
        stage("clang-debug:test-shuffle") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "mkdir ./clang-debug/run-shuffled"
            sh "./clang-debug/hyriseTest clang-debug/run-shuffled --gtest_repeat=5 --gtest_shuffle"
          } else {
            Utils.markStageSkippedForConditional("clangDebugRunShuffled")
          }
        }
      }, clangDebugTidy: {
        stage("clang-debug:tidy") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug-tidy && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          } else {
            Utils.markStageSkippedForConditional("clangDebugTidy")
          }
        }
      }, clangDebugSanitizers: {
        stage("clang-debug:sanitizers") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-debug-sanitizers && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "LSAN_OPTIONS=suppressions=.lsan-ignore.txt ASAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-debug-sanitizers/hyriseTest clang-debug-sanitizers"
          } else {
            Utils.markStageSkippedForConditional("clangDebugSanitizers")
          }
        }
      }, gccDebug: {
        stage("gcc-debug") {
          sh "export CCACHE_BASEDIR=`pwd`; cd gcc-debug && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
          sh "./gcc-debug/hyriseTest gcc-debug"
        }
      }, gccRelease: {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          stage("gcc-release") {
            sh "export CCACHE_BASEDIR=`pwd`; cd gcc-release && make all -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "./gcc-release/hyriseTest gcc-release"
          }
        } else {
            Utils.markStageSkippedForConditional("gccRelease")
        }
      }, clangSystemTestDebug: {
        stage("System Test") {
            sh "./clang-debug/hyriseSystemTest"
        }
      }, clangSystemTestRelease: {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          stage("System Test") {
              sh "./clang-release/hyriseSystemTest"
          }
        } else {
            Utils.markStageSkippedForConditional("clangSystemTestRelease")
          }
      }, clangReleaseSanitizers: {
        stage("clang-release:sanitizers") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-sanitizers && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "LSAN_OPTIONS=suppressions=.lsan-ignore.txt ASAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release-sanitizers/hyriseTest clang-release-sanitizers"
          } else {
            Utils.markStageSkippedForConditional("clangReleaseSanitizers")
          }
        }
      }, clangReleaseSanitizersNoNuma: {
        stage("clang-release:sanitizers w/o NUMA") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; cd clang-release-sanitizers-no-numa && make hyriseTest -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 3))"
            sh "LSAN_OPTIONS=suppressions=.lsan-ignore.txt ASAN_OPTIONS=suppressions=.asan-ignore.txt ./clang-release-sanitizers-no-numa/hyriseTest clang-release-sanitizers-no-numa"
          } else {
            Utils.markStageSkippedForConditional("clangReleaseSanitizersNoNuma")
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
      }, memcheckClangRelease: {
        stage("valgrind-memcheck-release") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "mkdir ./clang-release-memcheck"
            sh "valgrind --tool=memcheck --error-exitcode=1 --leak-check=full --gen-suppressions=all --suppressions=.valgrind-ignore.txt ./clang-release/hyriseTest clang-release-memcheck --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
          } else {
            Utils.markStageSkippedForConditional("memcheckClangRelease")
          }
        }
      }

      stage("Cleanup") {
        // Clean up workspace.
        script {
          githubNotify context: 'CI Pipeline', status: 'SUCCESS'
          if (env.BRANCH_NAME == 'master' || full_ci) {
            githubNotify context: 'Full CI', status: 'SUCCESS'
          }
        }
        step([$class: 'WsCleanup'])
      }
    } catch (error) {
      stage ("Cleanup after fail") {
        script {
          githubNotify context: 'CI Pipeline', status: 'FAILURE'
          if (env.BRANCH_NAME == 'master') {
            slackSend ":rotating_light: ALARM! Build on Master failed! - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>) :rotating_light:"
          }
        }
      }
      throw error
    } finally {

      sh "ls -A1 | xargs rm -rf"
      deleteDir()
    }
  }
}
