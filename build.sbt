name := "ikazuchi_zero"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.0"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.8.2.1"

resolvers += "Atilika Open Source repository" at "http://www.atilika.org/nexus/content/repositories/atilika"

libraryDependencies ++= Seq(
  "org.atilika.kuromoji" % "kuromoji" % "0.7.7",
  "com.opencsv" % "opencsv" % "3.7"
)