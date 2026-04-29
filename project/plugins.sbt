addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

// 2.0.x → scalac 2.0.7 (no _2.13.14). 2.2.0 → scalac 2.2.0 (no _2.13.16). 2.3.0 → scalac 2.3.0 for 2.12.18 / 2.13.16 (Spark 3.5.8+ aligns on 2.13.16 per SIP-51).
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// for resolving different scala-xml version requirements between sbt-scoverage and scalastyle-sbt-plugin.
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
