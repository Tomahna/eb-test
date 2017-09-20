import Dependencies._

organization := "fr.tomahna"
scalaVersion := "2.11.8"
version := "0.1.0-SNAPSHOT"
name := "eb-test"

javaOptions += "-Xmx2G"
fork := true

libraryDependencies += spark

libraryDependencies += scalaTest % Test
