archerJavaPipeline{
  // Pass Sonar Tests
  sonar_shuttle_stage(sources: ".", exclusions: "")  // -Dsonar.java.binaries=.

  // Generate Unit Tests Files
  stage("Generate Unit Tests"){
    container("jdk"){
      sh """
        env
        //mvn cobertura:cobertura
      """
    }
  }

  // Read JUnit Generated Files
  qa_data_shuttle_stage()

  // Deploy JAR
  stage("Deploy JAR"){
    container("jdk"){
      sh """
        mvn clean deploy
      """
    }
  }

  javadoc_shuttle_stage()
}
