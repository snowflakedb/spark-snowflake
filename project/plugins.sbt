addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

val sparkVersion = System.getProperty("sparkVersion", "unknown")

// Different scoverage versions for different generations of Spark builds.
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4").filter(_ => sparkVersion.startsWith("4"))
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12").filter(_ => sparkVersion.startsWith("3"))

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// for resolving different scala-xml version requirements between sbt-scoverage and scalastyle-sbt-plugin.
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
