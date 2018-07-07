import com.typesafe.sbt._
import Dependencies._

val basicSettings = Seq(
  shellPrompt           := { s => Project.extract(s).currentProject.id + " > " },
  version               := "0.6.0",
  scalaVersion          := "2.12.4",
  homepage              := Some(new URL("https://github.com/Tesco/mewbase")),
  organization          := "com.tesco",
  organizationHomepage  := Some(new URL("http://www.tesco.com")),
  description           := "Event Sourcing and CQRS Library for Java and Scala",
  startYear             := Some(2016),
  licenses              := Seq("MIT" -> new URL("https://github.com/Tesco/mewbase/blob/master/LICENSE.txt")),
  resolvers             ++= resolutionRepos,

  javacOptions          ++= Seq(
    "-deprecation",
    "-target", "1.8",
    "-source", "1.8",
    "-encoding", "utf8",
    "-Xlint:unchecked"
  ),

  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, scalaMajor)) if scalaMajor >= 10 =>
        Seq("-feature", "-language:implicitConversions", "-unchecked", "-deprecation", "-encoding", "utf8")
      case _ =>
        Seq.empty
    }
  },

  libraryDependencies  ++= Dependencies.test( junit, vertxUnit, restAssured, scalatest, commonsIo),

  // scaladoc settings
  (scalacOptions in doc) ++= Seq("-doc-title", name.value, "-doc-version", version.value),

  // publishing
  crossScalaVersions := Seq("2.10.7", "2.11.12", "2.12.4"),
  scalaBinaryVersion := {
    if (CrossVersion.isScalaApiCompatible(scalaVersion.value)) CrossVersion.binaryScalaVersion(scalaVersion.value)
    else scalaVersion.value
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  useGpg := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else                             Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  pomExtra :=
    <scm>
      <url>git@github.com:Tesco/mewbase</url>
      <connection>scm:git:git@github.com:Tesco/mewbase.git</connection>
    </scm>
    <developers>
      <developer>
        <id>Tesco</id>
        <name>Tesco</name>
      </developer>
    </developers>
)

val noPublishing = Seq(
  publishArtifact := false,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))

def javaDoc = Seq(
  doc in Compile := {
    val cp = (fullClasspath in Compile in doc).value
    val docTarget = (target in Compile in doc).value
    val compileSrc = (javaSource in Compile).value
    val s = streams.value
    def docLink = name.value match {
      case _ => ""
    }
    val cmd = "javadoc" +
            " -sourcepath " + compileSrc +
            " -classpath " + cp.map(_.data).mkString(":") +
            " -d " + docTarget +
            docLink +
            " -encoding utf8" +
            " -public" +
            " -windowtitle " + name.value + "_" + version.value +
            " -subpackages" +
            " io.mewbase"
    s.log.info(cmd)
    sys.process.Process(cmd) ! s.log
    docTarget
  }
)


lazy val root = Project("root", file("."))
  .aggregate(
    mewbaseCore,
    mewbaseJava,
    mewbaseScala,
    examplesJava,
    examplesScala,
    mewbaseRestHttp4s,
    mewbaseRestVertx,
    mewbaseRestIntegrationTest)
  .settings(basicSettings: _*)
  .settings(noPublishing: _*)


lazy val mewbaseCore = Project("mewbase-core", file("mewbase-core"))
  .settings(basicSettings: _*)
  .settings(javaDoc: _*)
  .settings(
    libraryDependencies ++= Dependencies.compile(
      jackson, jacksonData , jacksonBson,  // wire-encodings BSON and JSON
      slf4j, slf4jAPI, lbConfig, micrometer, // logging, config, metrics
      nats, artemis, kafka , // EventSource and/or Sink implementations
      postgres , lmdb  ,   // Binder implementations
      vertx, vertxAuth, vertxWeb, // REST frameworks,
    ),
    libraryDependencies ++= Dependencies.test(
      junit, junitIntf, vertxUnit, restAssured
    ),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q"),
    parallelExecution in Test := false,
    crossPaths := false,
    autoScalaLibrary := true
  )

lazy val mewbaseJava = Project("mewbase-java", file("mewbase-java"))
  .dependsOn(mewbaseCore)
  .settings(basicSettings: _*)
  .settings(javaDoc: _*)
  .settings(
    crossPaths := false,
    autoScalaLibrary := false
  )


lazy val mewbaseScala = Project("mewbase-scala", file("mewbase-scala"))
  .dependsOn(mewbaseCore)
  .settings(basicSettings: _*)
  .settings(
    libraryDependencies ++= Dependencies.compile(
      //"com.typesafe.akka" %% "akka-http" % "10.0.11"
    ),
    libraryDependencies ++= Dependencies.test(
      //"com.typesafe.akka" %% "akka-http-testkit" % "10.0.11"
    ),
    libraryDependencies ++= Dependencies.test(scalatest),

    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q"),
    crossPaths := true,
    autoScalaLibrary := true
  )

lazy val mewbaseRestHttp4s = Project("mewbase-rest-http4s", file("mewbase-rest-http4s"))
  .dependsOn(mewbaseCore)
  .settings(basicSettings: _*)
  .settings(
      libraryDependencies ++= http4s :+ vertx
  )

lazy val mewbaseRestVertx = Project("mewbase-rest-vertx", file("mewbase-rest-vertx"))
  .dependsOn(mewbaseCore)
  .settings(basicSettings: _*)
  .settings(
    libraryDependencies += vertx
  )


lazy val mewbaseRestIntegrationTest = Project("mewbase-rest-integrationtest", file("mewbase-rest-integrationtest"))
  .dependsOn(mewbaseCore, mewbaseRestHttp4s, mewbaseRestVertx)
  .settings(basicSettings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    libraryDependencies ++= http4s :+ http4sBlazeClient :+ http4sCirce :+ circeParser,
    libraryDependencies += scalatest % "test,it",
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testForkedParallel in Test := false,
    testForkedParallel in IntegrationTest := false,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    fork in IntegrationTest := false,
  )

lazy val examplesJava = Project("examples-java", file("examples-java"))
  .dependsOn(mewbaseJava, mewbaseRestVertx)
  .settings(basicSettings: _*)
  .settings(noPublishing: _*)

lazy val examplesScala = Project("examples-scala", file("examples-scala"))
  .dependsOn(mewbaseScala % "compile->compile;test->test", mewbaseRestHttp4s)
  .settings(basicSettings: _*)
  .settings(noPublishing: _*)
  .settings(
    libraryDependencies ++= Seq(circeParser, http4sCirce)
  )

