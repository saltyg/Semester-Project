name := "SparkExample"

version := "0.1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.slf4j" % "slf4j-api" % "1.7.5"
)

libraryDependencies += "com.twitter" %% "algebird-core" % "0.8.0"
libraryDependencies += "com.twitter" %% "algebird-spark" % "0.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1"
)
                            
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
