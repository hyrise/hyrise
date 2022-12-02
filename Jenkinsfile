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

  parallel clangDebugMacX64: {
    node('mac') {
      stage("clangDebugMacX64") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm
            
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

            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"

            sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${debug} ${unity} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@15/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@15/bin/clang++ .."
            sh "cd clang-debug && make -j \$(sysctl -n hw.logicalcpu)"
            sh "./clang-debug/hyriseTest"
            sh "./clang-debug/hyriseSystemTest --gtest_filter=\"-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4\""
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangDebugMacX64")
        }
      }
    }
  }, clangReleaseMacArm: {
    // For this to work, we installed a native non-standard JDK (zulu) via brew. See #2339 for more details.
    node('mac-arm') {
      stage("clangReleaseMacArm") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm
            
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
            
            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"
            
            // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
            sh "mkdir clang-release && cd clang-release && cmake ${release} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@15/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@15/bin/clang++ .."
            sh "cd clang-release && make -j \$(sysctl -n hw.logicalcpu)"

            // Check whether arm64 binaries are built to ensure that we are not accidentally running rosetta that
            // executes x86 binaries on arm.
            sh "file ./clang-release/hyriseTest | grep arm64"

            sh "./clang-release/hyriseTest"
            sh "./clang-release/hyriseSystemTest --gtest_filter=\"-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4\""
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangReleaseMacArm")
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
