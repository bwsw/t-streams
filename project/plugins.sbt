logLevel := Level.Warn
resolvers += "twitter-repo" at "https://maven.twttr.com"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.11.0")
