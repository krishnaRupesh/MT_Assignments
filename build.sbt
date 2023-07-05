name := "MT_Assignments"

version := "0.1"

scalaVersion := "2.12.15"


libraryDependencies += "io.delta" %% "delta-core" % "1.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.0"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.12.126"

