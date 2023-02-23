import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.TestTransactionConflicts'

try {
  node {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team) or the Jenkins admin user
      def cause = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0]
      def jenkinsUserName = cause ? cause['userId'] : null

      if (jenkinsUserName != "admin" && env.BRANCH_NAME != "master") {
        try {
          withCredentials([usernamePassword(credentialsId: 'github', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
            env.PR_CREATED_BY = pullRequest.createdBy
            sh '''
              curl -s -I -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/hyrise/hyrise/collaborators/${PR_CREATED_BY} | head -n 1 | grep "204"
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
    stage("Hostname") {
      // Print the hostname to let us know on which node the docker image was executed for reproducibility.
      sh "hostname"
    }
  
    // The empty '' results in using the default registry: https://index.docker.io/v1/
    docker.withRegistry('', 'docker') {
      def hyriseCI = docker.image('hyrise/hyrise-ci:22.04');
      hyriseCI.pull()

      // LSAN (executed as part of ASAN) requires elevated privileges. Therefore, we had to add --cap-add SYS_PTRACE.
      // Even if the CI run sometimes succeeds without SYS_PTRACE, you should not remove it until you know what you are doing.
      // See also: https://github.com/google/sanitizers/issues/764
      hyriseCI.inside("--cap-add SYS_PTRACE -u 0:0") {
        try {
          stage("Setup") {
            checkout scm

            // During CI runs, the user is different from the owner of the directories, which blocks the execution of git
            // commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be executed if
            // the corresponding directories are added as safe directories.
            sh '''
            git config --global --add safe.directory $WORKSPACE
            # Get the paths of the submodules; for each path, add it as a git safe.directory
            grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
            '''

            sh "./install_dependencies.sh"

            cmake = 'cmake -DCI_BUILD=ON'

            // We don't use unity builds with GCC 9 as it triggers https://github.com/google/googletest/issues/3552
            unity = '-DCMAKE_UNITY_BUILD=ON'
 
            // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
            // support older versions. We test the oldest LLVM version shipped with Ubuntu 22.04 (i.e., LLVM 11) and
            // GCC 9 (oldest version supported by Hyrise). We execute at least debug runs for them.
            // If you want to upgrade compiler versions, please update install_dependencies.sh,  DEPENDENCIES.md, and
            // the documentation (README, Wiki).
            clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
            clang11 = '-DCMAKE_C_COMPILER=clang-11 -DCMAKE_CXX_COMPILER=clang++-11'
            gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'
            gcc9 = '-DCMAKE_C_COMPILER=gcc-9 -DCMAKE_CXX_COMPILER=g++-9'

            debug = '-DCMAKE_BUILD_TYPE=Debug'
            release = '-DCMAKE_BUILD_TYPE=Release'
            relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

            // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the same time, we run into conflicts.
            // Thus, run this one (any one, really) first, so that the autoconf step can finish in peace.

            sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity}  .. && make -j libjemalloc-build"

            // Configure the rest in parallel
            sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}   ${unity} -DENABLE_CLANG_TIDY=ON .. &\
            mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
            mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}            -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
            mkdir clang-debug-addr-ub-sanitizers && cd clang-debug-addr-ub-sanitizers &&                 ${cmake} ${debug}          ${clang}            -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-release-addr-ub-sanitizers && cd clang-release-addr-ub-sanitizers &&             ${cmake} ${release}        ${clang}   ${unity} -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}            .. &\
            mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}            -DENABLE_THREAD_SANITIZATION=ON .. &\
            mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}              .. &\
            mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}              .. &\
            mkdir clang-11-debug && cd clang-11-debug &&                                                 ${cmake} ${debug}          ${clang11}          .. &\
            mkdir gcc-9-debug && cd gcc-9-debug &&                                                       ${cmake} ${debug}          ${gcc9}             .. &\
            wait"
          }

          parallel clangDebug: {
            stage("clang-debug") {
              sh "cd clang-debug && make all -j \$(( \$(nproc) / 4))"
              sh "./clang-debug/hyriseTest clang-debug"
            }
          }, clang11Debug: {
            stage("clang-11-debug") {
              sh "cd clang-11-debug && make all -j \$(( \$(nproc) / 4))"
              sh "./clang-11-debug/hyriseTest clang-11-debug"
            }
          }, gccDebug: {
            stage("gcc-debug") {
              sh "cd gcc-debug && make all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-debug && ./hyriseTest"
            }
          }, gcc9Debug: {
            stage("gcc-9-debug") {
              sh "cd gcc-9-debug && make all -j \$(( \$(nproc) / 4))"
              sh "cd gcc-9-debug && ./hyriseTest"
            }
          }, lint: {
            stage("Linting") {
              sh '''
                scripts/lint.sh
              '''
            }
          }, clangDebugCoverage: {
            stage("clang-debug-coverage") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "./scripts/coverage.sh --generate_badge=true"
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
        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
        }
      }
    }
  }

  node {
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
