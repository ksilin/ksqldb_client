// *****************************************************************************
// Projects
// *****************************************************************************

lazy val ksqldb_client =
  project
    .in(file("."))
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
        library.clients,
        library.kafka,
        library.ksqlDbClient,
        library.ksqlDbRestApp,
        //library.pureConfig,
        library.betterFiles,
        library.kantancsv,
        library.kantangeneric,
        library.circe,
        library.circeGeneric,
        library.circeParser,
        library.monix,
        library.sttp,
        library.sttpBackendOkHttp,
        library.sideEffectsProcessor,
        library.airframeLog,
        library.logback,
        library.wireMock % Test,
        library.ksqlDbTestUtil % Test,
        library.mockServer % Test,
        library.mockServerClient % Test,
        library.randomDataGen % Test,
        library.scalaFaker % Test,
        library.testcontainers % Test,
        library.scalatest % Test,
        library.restAssured % Test,
        library.restAssuredScala % Test,
      ),libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-reload4j")) }
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {
      val kafka = "3.2.0"
      val ksqlDb = "0.27.2"
      val pureConfig = "0.17.1"
      val betterFiles = "3.9.1"
      val kantan = "0.6.2"
      val circe = "0.14.2"
      val sttp = "3.7.6"
      val monix = "3.4.1"
      val scalacheckFaker ="7.0.0"
      val airframeLog = "22.8.0"
      val logback = "1.2.10"
      val scalatest = "3.2.13"
      val testContainers = "0.2.1"
      val restAssured = "5.1.1"
      val wiremock = "2.33.2"
      val mockServer = "5.14.0"
    }

    val clients = "org.apache.kafka" % "kafka-clients" % Version.kafka
    val kafka = "org.apache.kafka" %% "kafka" % Version.kafka
    val ksqlDbClient = "io.confluent.ksql" % "ksqldb-api-client" % Version.ksqlDb
    val ksqlDbTestUtil = "io.confluent.ksql" % "ksqldb-test-util" % Version.ksqlDb
    val ksqlDbRestApp = "io.confluent.ksql" % "ksqldb-rest-app" % Version.ksqlDb
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % Version.pureConfig
    val betterFiles = "com.github.pathikrit" %% "better-files" % Version.betterFiles
    val kantancsv = "com.nrinaudo" %% "kantan.csv" % Version.kantan
    val kantangeneric = "com.nrinaudo" %% "kantan.csv-generic" % Version.kantan
    val circe = "io.circe" %% "circe-core" % Version.circe
    val circeGeneric = "io.circe" %% "circe-generic" % Version.circe
    val circeParser = "io.circe" %% "circe-parser" % Version.circe
    val monix = "io.monix" %% "monix" % Version.monix
    val sttp                = "com.softwaremill.sttp.client3" %% "core"                      % Version.sttp
    val sttpBackendOkHttp   = "com.softwaremill.sttp.client3" %% "okhttp-backend"            % Version.sttp
    val airframeLog = "org.wvlet.airframe" %% "airframe-log" % Version.airframeLog
    val logback = "ch.qos.logback" % "logback-core" % Version.logback
    val sideEffectsProcessor = "blep" % "kafka-side-effects-processor" % "1.0-SNAPSHOT"
    val wireMock = "com.github.tomakehurst" % "wiremock" % Version.wiremock
    val mockServer = "org.mock-server" % "mockserver-netty" % Version.mockServer
    val mockServerClient = "org.mock-server" % "mockserver-client-java" % Version.mockServer
    val randomDataGen = "com.danielasfregola" %% "random-data-generator" % "2.9"
    val scalaFaker = "io.github.etspaceman" %% "scalacheck-faker" % Version.scalacheckFaker
    val testcontainers = "com.github.christophschubert" % "cp-testcontainers" % Version.testContainers
    val restAssured = "io.rest-assured" % "rest-assured" % Version.restAssured
    val restAssuredScala = "io.rest-assured" % "scala-support" % Version.restAssured
    val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.8",
    organization := "example.com",
    organizationName := "ksilin",
    startYear := Some(2020),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding", "UTF-8",
      "-Ywarn-unused:imports",
    ),
    javaOptions ++= Seq("-J--illegal-access=allow", "-J--add-opens=java.base/java.util=ALL-UNNAMED",
      "-J--add-opens java.base/java.util=ALL-UNNAMED"),

    // find latest repo here: https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/
    resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven",
      "ksqlDb" at "https://ksqldb-maven.s3.amazonaws.com/maven",
      Resolver.sonatypeRepo("releases"),
      Resolver.bintrayRepo("wolfendale", "maven"),
      Resolver.mavenLocal,
      "jitpack" at "https://jitpack.io"
    ),
    scalafmtOnCompile := true,
    Test / fork  := true, // required for setting env vars
    // envVars in Test := Map("RANDOM_DATA_GENERATOR_SEED" -> "9153932137467828920"),
  )

Compile / avroSourceDirectories += (Compile / resourceDirectory).value / "avro"
Compile / avroSpecificSourceDirectories += (Compile / resourceDirectory).value / "avro"
Compile / sourceGenerators += (Compile / avroScalaGenerateSpecific).taskValue