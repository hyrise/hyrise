import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')

// Due to their long runtime, we skip several tests in sanitizer and MacOS builds.
tests_excluded_in_sanitizer_builds = 'SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:SSBTableGeneratorTest.GenerateAndStoreRowCounts'

tests_excluded_in_mac_builds = 'TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:SSBTableGeneratorTest.GenerateAndStoreRowCounts'

// We run the strict ("more aggressive") checks for the address sanitizer
// (see https://github.com/google/sanitizers/wiki/AddressSanitizer#faq). Moreover, we activate the leak sanitizer.
asan_options = 'strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1'

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
          stage ("User unknown.") {
            script {
              githubNotify context: 'CI Pipeline', status: 'FAILURE', description: 'User is not a collaborator.'
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

  parallel clangMacX64: {
    node('mac') {
      stage("clangMacX64") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            // We have experienced frequent network problems with this CI machine. So far, we have not found the cause.
            // Since we run this stage very late and it frequently fails due to network problems, we retry pulling the
            // sources five times to avoid re-runs of entire Full CI runs that failed in the very last stage due to a
            // single network issue.
            retry(5) {
              try {
                checkout scm

                // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container.
                sh "git submodule update --init --recursive --jobs 4 --depth=1"
              } catch (error) {
                sh "ls -A1 | xargs rm -rf"
                throw error
              }
            }

            // Build hyriseTest (Debug) with macOS's default compiler (Apple clang) and run it.
            sh "mkdir clang-apple-debug && cd clang-apple-debug && PATH=/usr/local/bin/:$PATH cmake ${debug} ${unity} ${ninja} .."
            sh "cd clang-apple-debug && PATH=/usr/local/bin/:$PATH ninja"
            sh "./clang-apple-debug/hyriseTest"

            // Build Hyrise (Release) with a recent clang compiler version (as recommended for Hyrise on macOS) and run
            // various tests.
            sh "mkdir clang-release && cd clang-release && PATH=/usr/local/bin/:$PATH /usr/local/bin/cmake ${release} ${ninja} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@17/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@17/bin/clang++ .."
            sh "cd clang-release && PATH=/usr/local/bin/:$PATH ninja"
            sh "./clang-release/hyriseTest"
            sh "./clang-release/hyriseSystemTest --gtest_filter=-${tests_excluded_in_mac_builds}"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangMacX64")
        }
      }
    }
  }, clangMacArm: {
    // For this to work, we installed a native non-standard JDK (zulu) via brew. See #2339 for more details.
    node('mac-arm') {
      stage("clangMacArm") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm

            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"

            // Build hyriseTest (Release) with macOS's default compiler (Apple clang) and run it.
            sh "mkdir clang-apple-release && cd clang-apple-release && cmake ${release} ${ninja} .."
            sh "cd clang-apple-release && ninja"
            sh "./clang-apple-release/hyriseTest"

            // Build Hyrise (Debug) with a recent clang compiler version (as recommended for Hyrise on macOS) and run
            // various tests.
            // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
            sh "mkdir clang-debug && cd clang-debug && cmake ${debug} ${unity} ${ninja} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@17/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@17/bin/clang++ .."
            sh "cd clang-debug && ninja"

            // Check whether arm64 binaries are built to ensure that we are not accidentally running rosetta that
            // executes x86 binaries on arm.
            sh "file ./clang-debug/hyriseTest | grep arm64"

            sh "./clang-debug/hyriseTest"
            sh "./clang-debug/hyriseSystemTest --gtest_filter=-${tests_excluded_in_mac_builds}"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-debug"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangMacArm")
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
