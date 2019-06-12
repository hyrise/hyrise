import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')

try {
  node('master') {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team)
      if (env.CHANGE_ID) {
        try {
          withCredentials([usernamePassword(credentialsId: '5fe8ede9-bbdb-4803-a307-6924d4b4d9b5', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
            env.PR_CREATED_BY = pullRequest.createdBy
            sh '''
              curl -s -I -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/hyrise/hyrise/collaborators/${PR_CREATED_BY} | head -n 1 | grep "HTTP/1.1 204 No Content"
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
  node('mac') {
    stage("clangDebugMac") {
      try {
        checkout scm

        sh "pwd"

        // We do not use install.sh here as there is no way to run OS X in a Docker container
        sh "git submodule update --init --recursive --jobs 4"

        sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake -DCMAKE_C_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/local/Cellar/llvm\\@7/7.0.1/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/Cellar/llvm\\@7/7.0.1/bin/clang++ .."
        sh "cd clang-debug && PATH=/usr/local/bin:$PATH make -j libjemalloc-build"
        sh "cd clang-debug && CCACHE_CPP2=yes CCACHE_SLOPPINESS=file_macro CCACHE_BASEDIR=`pwd` make -j8"
        sh "./clang-debug/hyriseTest"
      } finally {
        sh "ls -A1 | xargs rm -rf"
      }
    }
  }

  node ('master') {
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
