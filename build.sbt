name := "spark-essentials"
version := "0.1"
scalaVersion := "2.13.6"

val sparkVersion = "3.2.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.6.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1",
  "io.delta" %% "delta-core" % "1.1.0",
  "org.apache.spark" %% "spark-avro" % "3.2.1"
)