name := "PieelStreamingProject"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Atilika Open Source repository" at "http://www.atilika.org/nexus/content/repositories/atilika"

libraryDependencies ++= Seq(
  "org.atilika.kuromoji" % "kuromoji" % "0.7.7"
  "com.opencsv" % "opencsv" % "3.7"
)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "1.6.0").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("com.google.guava","guava").
    exclude("org.apache.hadoop","hadoop-yarn-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("org.spark-project.spark", "unused").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("javax.xml.bind", "jsr173_api") //javax.xml.bind/jsr173_api/jars/jsr173_api-1.0.jar
)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-streaming" % "1.6.0").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("com.google.guava","guava").
    exclude("org.apache.hadoop","hadoop-yarn-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("org.spark-project.spark", "unused").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("javax.xml.bind", "jsr173_api") //javax.xml.bind/jsr173_api/jars/jsr173_api-1.0.jar
)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-streaming-twitter" % "1.6.0").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("com.google.guava","guava").
    exclude("org.apache.hadoop","hadoop-yarn-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("org.spark-project.spark", "unused").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("javax.xml.bind", "jsr173_api") //javax.xml.bind/jsr173_api/jars/jsr173_api-1.0.jar
)

libraryDependencies ++= Seq(
  ("org.apache.kafka" %% "kafka" % "0.8.2.1")
)


mainClass in assembly := Some("PieelStreaming")
.opencsv" % "opencsv" % "3.7"
)
