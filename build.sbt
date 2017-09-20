import Dependencies._

organization := "fr.tomahna"
scalaVersion := "2.11.8"
version := "0.1.0-SNAPSHOT"
name := "eb-test"

libraryDependencies += spark

libraryDependencies += scalaTest % Test
