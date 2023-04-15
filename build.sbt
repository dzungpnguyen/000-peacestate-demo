name := "Spark Dataframe 1"
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"
version := "1.0"

scalaVersion := "2.13.8"
val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion