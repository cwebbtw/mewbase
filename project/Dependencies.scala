import sbt._

object Dependencies {

  val resolutionRepos = Seq( )
  val Http4sVersion = "0.18.12"

  def compile   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")
  def provided  (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "provided")
  def test      (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")
  def runtime   (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "runtime")
  def container (deps: ModuleID*): Seq[ModuleID] = deps map (_ % "container")

  // include for core

  // JSON and BSON encodings
  val jackson     = "com.fasterxml.jackson.core" % "jackson-core" % "2.7.4"
  val jacksonData = "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.4"
  val jacksonBson = "de.undercouch" % "bson4jackson" % "2.7.0"

  val http4sBlaze = "org.http4s"      %% "http4s-blaze-server" % Http4sVersion
  val http4sCirce = "org.http4s"      %% "http4s-circe"        % Http4sVersion
  val http4sDsl = "org.http4s"      %% "http4s-dsl"          % Http4sVersion
  val http4sClient = "org.http4s"      %% "http4s-client"        % Http4sVersion

  val http4s = Seq(http4sBlaze, http4sDsl, http4sClient)

  // Logging  Config and Metrics
  val slf4j       = "org.slf4j" % "slf4j-log4j12" % "1.7.21"
  val slf4jAPI    = "org.slf4j" % "slf4j-api" % "1.7.21"
  val lbConfig    = "com.typesafe" % "config" % "1.3.1"
  val micrometer  =  "io.micrometer" % "micrometer-core" % "1.0.4"

  // Various dependencies for implementations of Event and Binder servers
  // EventSource and/or Sink implementations
  val nats        = "io.nats" % "java-nats-streaming" % "0.4.1"
  val artemis     = "org.apache.activemq" % "artemis-jms-client" % "2.4.0"
  val kafka       = "org.apache.kafka" % "kafka-clients" % "1.0.0"

  // Binder implementations
  val lmdb        = "org.lmdbjava" % "lmdbjava" % "0.6.0"
  val postgres    = "org.postgresql" % "postgresql" % "42.1.4"

  // REST framework
  val vertxWeb    = "io.vertx" % "vertx-web" % "3.4.2"
  val vertx       = "io.vertx" % "vertx-core" % "3.4.2"
  val vertxAuth   = "io.vertx" % "vertx-auth-common" % "3.4.2"

  // Java Test frameworks
  val junit       = "junit" % "junit" % "4.12"
  val junitIntf   = "com.novocode" % "junit-interface" % "0.11"
  val vertxUnit   = "io.vertx" % "vertx-unit" % "3.4.2"
  val restAssured = "io.rest-assured" % "rest-assured" % "3.0.6"

  val commonsIo = "commons-io" % "commons-io" % "2.6"

  // Scala Test
  def scalatest(scalaVersion: String) = "org.scalatest" %% "scalatest" % "3.0.5"


}
