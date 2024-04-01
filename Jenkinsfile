import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
// Exclude some tests that are too long-running and are not executed in a transactional world.
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor'

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
        // We have experienced frequent network problems with this CI machine. So far, we have not found the cause.
        // Since we run this stage very late and it frequently fails due to network problems, we retry the stage three
        // times as (i) we can be rather sure that most problems with the current pull request have already been found
        // in earlier stages and fails in this stage are probably network issues, and (ii) we avoid re-runs of entire
        // Full CI runs that failed in the very last stage due to a single network issue.
        retry(3) {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            try {
              checkout scm

              // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
              sh "git submodule update --init --recursive --jobs 4 --depth=1"

              // Build hyriseTest with macOS's default compiler (Apple clang) and run it.
              sh "mkdir clang-apple-debug && cd clang-apple-debug && /usr/local/bin/cmake ${debug} ${unity} .."
              sh "cd clang-apple-debug && make -j \$(sysctl -n hw.logicalcpu)"
              sh "./clang-apple-debug/hyriseTest"

              // Build Hyrise with a recent clang compiler version (as recommended for Hyrise on macOS) and run various tests.
              sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${debug} ${unity} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@16/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@16/bin/clang++ .."
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

              // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
              sh "git submodule update --init --recursive --jobs 4 --depth=1"

              // Build hyriseTest with macOS's default compiler (Apple clang) and run it.
              sh "mkdir clang-apple-release && cd clang-apple-release && cmake ${release} .."
              sh "cd clang-apple-release && make -j \$(sysctl -n hw.logicalcpu)"
              sh "./clang-apple-release/hyriseTest"

              // Build Hyrise with a recent clang compiler version (as recommended for Hyrise on macOS) and run various tests.
              // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
              sh "mkdir clang-release && cd clang-release && cmake ${release} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@16/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@16/bin/clang++ .."
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
