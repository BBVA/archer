archerJavaPipeline{
  // Pass Sonar Tests
  sonar_shuttle_stage(sources: ".", exclusions: "")  // -Dsonar.java.binaries=.

  // Read JUnit Generated Files
  qa_data_shuttle_stage()

  // Deploy JAR
  stage("Deploy JAR"){
    container("jdk"){
      sh """
        ./gradlew publish
      """
    }
  }

  javadoc_shuttle_stage()
}
