import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-svd-item-similarity"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % pioVersion.value % "provided",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.3",
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.Beta4"
    exclude("org.apache.spark", "spark-catalyst_2.10")
    exclude("org.apache.spark", "spark-sql_2.10"),
  "org.apache.spark" %% "spark-core"    % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.3.0" % "provided",
  "org.xerial.snappy" % "snappy-java" % "1.1.1.7")
