import com.typesafe.sbt._
import Dependencies._


val basicSettings = Seq(
  shellPrompt           := { s => Project.extract(s).currentProject.id + " > " },
  version               := "0.6.0",
  scalaVersion          := "2.12.4",
  homepage              := Some(new URL("https://github.com/Tesco/mewbase")),
  organization          := "io.mewbase",
  organizationHomepage  := Some(new URL("http://www.tesco.com")),
  description           := "Event Sourcing and CQRS Library for the JVM",
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

  libraryDependencies  ++= Dependencies.test( junit, vertxUnit, restAssured, scalatest(scalaVersion.value), commonsIo),

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
    <repositories>
      <repository>
        <id>sbtage</id>
        <name>sbtage staging repo</name>
        <url>http://sbtage.com/maven2/</url>
      </repository>
    </repositories>
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


lazy val root = Project("mewbase", file("."))
  .aggregate(mewbaseCore, mewbaseJava, mewbaseScala, examplesJava, examplesScala)
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
      vertx, vertxAuth, vertxWeb // REST frameworks

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
    libraryDependencies ++= Dependencies.test(scalatest(scalaVersion.value)),

    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q"),
    crossPaths := true,
    autoScalaLibrary := true
  )


lazy val examplesJava = Project("examples-java", file("examples-java"))
  .dependsOn(mewbaseJava)
  .settings(basicSettings: _*)
  .settings(noPublishing: _*)


lazy val examplesScala = Project("examples-scala", file("examples-scala"))
  .dependsOn(mewbaseScala % "compile->compile;test->test")
  .settings(basicSettings: _*)
  .settings(noPublishing: _*)
