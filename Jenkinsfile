node {

  docker.image('hyrise/opossum-ci:16.10').inside("-u 0:0") {

    stage("Setup") {
      step([$class: 'WsCleanup'])
      checkout scm
      sh "./install.sh"
      sh "git submodule update --init"
    }

    stage("Test gcc") {
      stage("gcc Release") {
        sh "premake4 --compiler=gcc"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release test"
      }
      stage("gc Debug") {
        sh "make clean"
        sh "premake4 --compiler=gcc"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug test"
      }
    }

    stage("Test clang") {
      stage("clang Release") {
        sh "make clean"
        sh "premake4 --compiler=clang"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release test"
      }
      stage("clang Debug") {
        sh "make clean"
        sh "premake4 --compiler=clang"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug test"
      }
    }

    stage("ASAN") {
      stage("asan Release") {
        sh "make clean"
        sh "premake4 --compiler=clang"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=release asan"
      }
      stage("asan Debug") {
        sh "make clean"
        sh "premake4 --compiler=clang"
        sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) -R config=debug asan"
      }
    }

    stage("Coverage") {
      sh "make clean"
      sh "premake4"
      sh "make -j \$(cat /proc/cpuinfo | grep processor | wc -l) coverage"
      publishHTML (target: [
        allowMissing: false,
        alwaysLinkToLastBuild: false,
        keepAll: true,
        reportDir: 'coverage',
        reportFiles: 'index.html',
        reportName: "RCov Report"
      ])
    }

    stage("Cleanup") {
      // Clean up workspace
      step([$class: 'WsCleanup'])
    }

  }



}