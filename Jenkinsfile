import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
tests_excluded_in_sanitizer_builds = '--gtest_filter=-SQLiteTestRunnerEncodings/*:TpcdsTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor'

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

  node('linux') {
    def oppossumCI = docker.image('hyrise/opossum-ci:19.10');
    oppossumCI.pull()
    // create ccache volume on host using:
    // mkdir /mnt/ccache; mount -t tmpfs -o size=200G none /mnt/ccache
    // or add it to /etc/fstab:
    // tmpfs  /mnt/ccache tmpfs defaults,size=200G  0 0

    oppossumCI.inside("-u 0:0 -v /mnt/ccache:/ccache -e \"CCACHE_DIR=/ccache\" -e \"CCACHE_MAXSIZE=200GB\" -e \"CCACHE_SLOPPINESS=file_macro,pch_defines,time_macros\" -e\"CCACHE_DEPEND=1\" -e\"CCACHE_NOHASHDIR=1\" -e\"CCACHE_DEBUG=1\" --privileged=true") {
      try {
        stage("Setup") {
          checkout scm
          sh "./install_dependencies.sh"

          cmake = 'cmake -DCI_BUILD=ON'
          ccache = '-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache'
          unity = '-DCMAKE_UNITY_BUILD=ON'

          clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
          gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'

          debug = '-DCMAKE_BUILD_TYPE=Debug'
          release = '-DCMAKE_BUILD_TYPE=Release'
          relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'
        }
      } finally {
        sh "ls -A1 | xargs rm -rf"
        deleteDir()
      }
    }
  }

  // I have not found a nice way to run this in parallel with the steps above, as those are in a `docker.inside` block and this is not.
  node('mac') {
    stage("clangDebugMac") {
      if (env.BRANCH_NAME == 'master' || full_ci) {
        try {
          checkout scm

          // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
          sh "git submodule update --init --recursive --jobs 4"

          sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${unity} ${debug} -DCMAKE_C_COMPILER=/usr/local/Cellar/llvm/9.0.0/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/Cellar/llvm/9.0.0/bin/clang++ .."
          sh "cd clang-debug && PATH=/usr/local/bin:$PATH make -j libjemalloc-build"
          sh "cd clang-debug && make -j8"
          sh "./clang-debug/hyriseTest"
          sh "./clang-debug/hyriseSystemTest --gtest_filter=-TPCCTest*:TpcdsTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4"
          sh "./scripts/test/hyriseConsole_test.py clang-debug"
          sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
        } finally {
          sh "ls -A1 | xargs rm -rf"
        }
      } else {
        Utils.markStageSkippedForConditional("clangDebugMac")
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
