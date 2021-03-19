import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor'

unity = '-DCMAKE_UNITY_BUILD=ON'
debug = '-DCMAKE_BUILD_TYPE=Debug'
release = '-DCMAKE_BUILD_TYPE=Release'
relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

try {
  node {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team) or the Jenkins admin user
      def cause = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0]
      def jenkinsUserName = cause ? cause['userId'] : null

      if (jenkinsUserName != "admin" && env.BRANCH_NAME != "master") {
        try {
          withCredentials([usernamePassword(credentialsId: '5fe8ede9-bbdb-4803-a307-6924d4b4d9b5', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
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

  // I have not found a nice way to run this in parallel with the steps above, as those are in a `docker.inside` block and this is not.
  node('mac-arm') {
    stage("clangDebugMacArm") {
      if (env.BRANCH_NAME == 'master' || full_ci) {
        try {
          checkout scm

          // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
          sh "git submodule update --init --recursive --jobs 4 --depth=1"
          
          // (Martin) testing
          sh "printenv"
          sh "sysctl sysctl.proc_translated"

          // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
          sh "mkdir clang-debug && cd clang-debug && HOMEBREW_PREFIX=/opt/homebrew HOMEBREW_REPOSITORY=/opt/homebrew HOMEBREW_CELLAR=/opt/homebrew/Cellar INFOPATH=/opt/homebrew/share/info: PATH=/opt/homebrew/bin:$PATH cmake ${unity} ${debug} -DBOOST_ROOT:PATHNAME=/opt/homebrew/include -DPostgreSQL_LIBRARY_DIR=/opt/homebrew/lib/ -DPostgreSQL_LIBRARY=/opt/homebrew/lib/ -DPostgreSQL_TYPE_INCLUDE_DIR=/opt/homebrew/Cellar/postgresql/13.2_1/include/postgresql/server/ -DPostgreSQL_INCLUDE_DIR=/opt/homebrew/Cellar/postgresql/13.2_1/include -DCMAKE_C_COMPILER=/opt/homebrew/Cellar/llvm/11.1.0/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/Cellar/llvm/11.1.0/bin/clang++ .."
          sh "cd clang-debug && make -j8"
          sh "./clang-debug/hyriseTest"
          sh "./clang-debug/hyriseSystemTest --gtest_filter=-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4"
          sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-debug"
          sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-debug"
          sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
        } finally {
          sh "ls -A1 | xargs rm -rf"
        }
      } else {
        Utils.markStageSkippedForConditional("clangDebugMacArm")
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
