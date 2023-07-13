name := "spark_scala_application"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3" % "provided"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.2" % "provided"

libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3" % "provided"