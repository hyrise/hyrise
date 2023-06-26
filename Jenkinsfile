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

            // Configure the rest in parallel.
            // Note on the clang-debug-tidy stage: clang-tidy misses some flaws when running in a unity build. However, it runs very long and we agreed to life with that for now.
            // See: https://gitlab.kitware.com/cmake/cmake/-/issues/20058
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
          }

        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
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
