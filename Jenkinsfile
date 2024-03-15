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

  node('linux') {
    stage("Hostname") {
      // Print the hostname to let us know on which node the docker image was executed for reproducibility.
      sh "hostname"
    }

    // The empty '' results in using the default registry: https://index.docker.io/v1/
    docker.withRegistry('', 'docker') {
      def hyriseCI = docker.image('hyrise/hyrise-ci:23.10');
      hyriseCI.pull()

      // LSAN (executed as part of ASAN) requires elevated privileges. Therefore, we had to add --cap-add SYS_PTRACE.
      // Even if the CI run sometimes succeeds without SYS_PTRACE, you should not remove it until you know what you are
      // doing. See also: https://github.com/google/sanitizers/issues/764
      hyriseCI.inside("--cap-add SYS_PTRACE -u 0:0") {
        try {
          stage("Setup") {
            checkout scm

            // Check if ninja-build is installed. As make is sufficient to work with Hyrise, ninja-build is not
            // installed via install_dependencies.sh but is part of the hyrise-ci docker image.
            sh "ninja --version > /dev/null"

            // During CI runs, the user is different from the owner of the directories, which blocks the execution of
            // git commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be
            // executed if the corresponding directories are added as safe directories.
            sh '''
            git config --global --add safe.directory $WORKSPACE
            # Get the paths of the submodules; for each path, add it as a git safe.directory
            grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
            '''

            sh "./install_dependencies.sh"

            cmake = 'cmake -DCI_BUILD=ON'

            // We don't use unity builds with clang-tidy (see below) and GCC 11 builds as it triggers a GoogleTest
            // issue (see https://github.com/google/googletest/issues/3552).
            unity = '-DCMAKE_UNITY_BUILD=ON'

            // To speed compiling up, we use ninja for most builds.
            ninja = '-GNinja'

            // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
            // support older versions. We test LLVM 15 (oldest LLVM version shipped with Ubuntu 23.10 that works with
            // more recent libstdc++ versions) and GCC 11 (oldest version supported by Hyrise). We execute at least
            // debug runs for them. If you want to upgrade compiler versions, please update install_dependencies.sh,
            // DEPENDENCIES.md, and the documentation (README, Wiki).
            clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
            clang15 = '-DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15'
            gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'
            gcc11 = '-DCMAKE_C_COMPILER=gcc-11 -DCMAKE_CXX_COMPILER=g++-11'

            debug = '-DCMAKE_BUILD_TYPE=Debug'
            release = '-DCMAKE_BUILD_TYPE=Release'
            relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

            // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the
            // same time, we run into conflicts. Thus, run this one (any one, really) first, so that the autoconf step
            // can finish in peace.
            sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity}  ${ninja} .. && ninja libjemalloc-build"

            // Configure the rest in parallel. We use unity builds to decrease build times, except for two
            // configurations: (1) clang tidy as it might otherwise miss issues with unity builds (e.g., missing
            // includes) and (2) GCC 11 debug builds of GTest (see https://github.com/google/googletest/issues/3552).
            sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}             ${ninja} -DENABLE_CLANG_TIDY=ON .. &\
            mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
            mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
            mkdir clang-debug-addr-ub-leak-sanitizers && cd clang-debug-addr-ub-leak-sanitizers &&       ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DENABLE_ADDR_UB_LEAK_SANITIZATION=ON .. &\
            mkdir clang-release-addr-ub-leak-sanitizers && cd clang-release-addr-ub-leak-sanitizers &&   ${cmake} ${release}        ${clang}   ${unity}  ${ninja} -DENABLE_ADDR_UB_LEAK_SANITIZATION=ON .. &\
            mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}   ${unity}  ${ninja} -DENABLE_THREAD_SANITIZATION=ON .. &\
            mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}   ${unity}  ${ninja} .. &\
            mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}     ${unity}           .. &\
            mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}     ${unity}  ${ninja} .. &\
            mkdir clang-15-debug && cd clang-15-debug &&                                                 ${cmake} ${debug}          ${clang15} ${unity}  ${ninja} .. &\
            mkdir gcc-11-debug && cd gcc-11-debug &&                                                     ${cmake} ${debug}          ${gcc11}             ${ninja} .. &\
            wait"
          }

          parallel clangDebug: {
            stage("clang-debug") {
              sh "cd clang-debug && ninja all -j \$(( \$(nproc) / 5))"
              sh "./clang-debug/hyriseTest clang-debug"
            }
          }, clang15Debug: {
            stage("clang-15-debug") {
              sh "cd clang-15-debug && ninja all -j \$(( \$(nproc) / 5))"
              sh "./clang-15-debug/hyriseTest clang-15-debug"
            }
          }, gccDebug: {
            stage("gcc-debug") {
              // We build gcc-debug using make.
              sh "cd gcc-debug && make all -j \$(( \$(nproc) / 5))"
              sh "cd gcc-debug && ./hyriseTest"
            }
          }, gcc11Debug: {
            stage("gcc-11-debug") {
               // We give more cores (ncores / 2.5) to GCC 11 as it is the only configuration that has issues with unity
               // builds (GoogleTest cannot be compiled). When switching to a more recent GCC version, this should be
               // evaluated again.
              sh "cd gcc-11-debug && ninja all -j \$(( \$(nproc) * 2 / 5))"
              sh "cd gcc-11-debug && ./hyriseTest"
            }
          }, lint: {
            stage("Linting") {
              sh "scripts/lint.sh"
            }
          }
        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
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
            sh "mkdir clang-apple-debug && cd clang-apple-debug && PATH=/usr/local/bin/:$PATH cmake ${debug} ${unity} ${ninja} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
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
            sh "mkdir clang-apple-release && cd clang-apple-release && cmake ${release} ${ninja} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ .."
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
