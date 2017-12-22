logLevel := Level.Warn

addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "17.10.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.12")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.6"