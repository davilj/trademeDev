lazy val root = (project in file(".")).
  settings(
    name := "sparkListingFunctions",
    version := "1.0",
    scalaVersion := "2.10.4"
)


libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % "1.4.1",
    "org.apache.spark" % "spark-sql_2.10" % "1.4.1",
	"org.scalatest" % "scalatest_2.10" % "2.0" % "test",
	//"org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
    "log4j" % "log4j" % "1.2.14"
)