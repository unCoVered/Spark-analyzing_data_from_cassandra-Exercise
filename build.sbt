name := "spark-analyzing_data_from_cassandra-exercise"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
