archerJavaPipeline{
  // Pass Sonar Tests
  sonar_shuttle_stage(sources: ".", exclusions: "")  // -Dsonar.java.binaries=.

  // Read JUnit Generated Files
  qa_data_shuttle_stage()

  // Deploy JAR
  stage("Deploy JAR"){
    container("jdk"){
      sh """
        export AWS_ACCESS_KEY_ID=AKIAIM5BHCDAPR2HJ6GQ
        export AWS_SECRET_ACCESS_KEY=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
        mvn clean deploy
      """
    }
  }

  javadoc_shuttle_stage()
}
