import Common._

val baseSettings = Seq(
  name := "t-streams",
  version := "2.2.5.1-SNAPSHOT",
  scalaVersion := "2.12.1",
  organization := "com.bwsw",
  organizationName := "Bitworks Software, Ltd.",
  organizationHomepage := Some(url("https://bitworks.software"))
)

lazy val root = project
  .in(file("."))
  .settings(
    baseSettings ++ projectSettings:_*
  )
