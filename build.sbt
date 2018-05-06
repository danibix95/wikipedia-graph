name := "wikipedia-graph"

version := "0.1"

scalaVersion := "2.11.6"

val sparkVer = "2.3.0"
libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "com.databricks" %% "spark-xml" % "0.4.1",
    "org.parboiled" %% "parboiled" % "2.1.4"
  )

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVer + "_" + module.revision + "." + artifact.extension
}