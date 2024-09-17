ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "backpacker-assignment",
    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.apache.spark" %% "spark-hive" % "3.3.0",    // Hive support
      "org.apache.spark" %% "spark-avro" % "3.3.0",    // Avro support (optional)

      // Hadoop dependencies for HDFS and Hive
      "org.apache.hadoop" % "hadoop-common" % "3.3.1",
//      "org.apache.hadoop" % "hadoop-hive" % "3.1.2",

      // Hadoop AWS for S3 integration
      "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.901",


// Typesafe Config for external configuration management
      "com.typesafe" % "config" % "1.4.1",

      // Logging dependencies
      "org.slf4j" % "slf4j-api" % "1.7.30",                // SLF4J API
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",            // SLF4J Log4j binding
      "log4j" % "log4j" % "1.2.17",                        // Log4j for logging

      // Optional: Spark testing dependencies (if you want to write tests)
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",   // For testing
      "org.apache.spark" %% "spark-sql" % "3.3.0" % "test" // Spark SQL for tests
    ),

    // Java 11 설정 (target JVM 버전 11로 설정)
    scalacOptions ++= Seq(
      "-target:jvm-11",
      "-deprecation",
      "-feature"
    ),

    // Enable S3 Support
    resolvers ++= Seq(
      "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/"
    )
  )
