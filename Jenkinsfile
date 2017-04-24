node {
  stage("Main build") {

      checkout scm

      docker.image('hyrise/opossum-ci:16.10').inside {

        stage("Test") {
          sh "echo success"
        }
      }

  }

  // Clean up workspace
  step([$class: 'WsCleanup'])

}