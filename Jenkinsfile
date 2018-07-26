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



      parallel clangDebugRun: {
        
      }, clangDebugCoverage: {
        stage("clang-debug-coverage") {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            sh "export CCACHE_BASEDIR=`pwd`; ./scripts/coverage.sh --generate_badge=true --launcher=ccache"
            sh "find coverage -type d -exec chmod +rx {} \;"
            archive 'coverage_badge.svg'
            archive 'coverage_percent.txt'
            publishHTML (target: [
              allowMissing: false,
              alwaysLinkToLastBuild: true,
              keepAll: true,
              reportDir: "coverage/coverage",
              includes: '**/*',
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

      ///sh "ls -A1 | xargs rm -rf"
      ///deleteDir()
    }
  }
}
