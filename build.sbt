name := "spark-sources-examples"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.3"
libraryDependencies += "org.joda" % "joda-convert" % "1.8"


libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.2.2"
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"
libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.2"