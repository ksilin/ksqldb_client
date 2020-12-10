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
        library.pureConfig,
        library.betterFiles,
        library.kantancsv,
        library.kantangeneric,
        library.circe,
        library.circeGeneric,
        library.circeParser,
        library.monix,
        library.sttp,
        library.sttpBackendOkHttp,
        library.airframeLog,
        library.wireMock % Test,
        library.mockServer % Test,
        library.mockServerClient % Test,
        library.randomDataGen % Test,
        library.scalaFaker % Test,
        library.scalatest % Test,
      ),
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {
      val kafka = "2.6.0"
      val ksqlDb = "0.13.0"
      val scalatest = "3.2.0"
      val pureConfig = "0.14.0"
      val betterFiles = "3.9.1"
      val kantan = "0.6.1"
      val circe = "0.13.0"
      val sttp = "3.0.0-RC9"
      val airframeLog = "20.12.1"
    }

    val clients = "org.apache.kafka" % "kafka-clients" % Version.kafka
    val kafka = "org.apache.kafka" %% "kafka" % Version.kafka
    val ksqlDbClient = "io.confluent.ksql" % "ksqldb-api-client" % Version.ksqlDb
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % Version.pureConfig
    val betterFiles = "com.github.pathikrit" %% "better-files" % Version.betterFiles
    val kantancsv = "com.nrinaudo" %% "kantan.csv" % Version.kantan
    val kantangeneric = "com.nrinaudo" %% "kantan.csv-generic" % Version.kantan
    val circe = "io.circe" %% "circe-core" % Version.circe
    val circeGeneric = "io.circe" %% "circe-generic" % Version.circe
    val circeParser = "io.circe" %% "circe-parser" % Version.circe
    val monix = "io.monix" %% "monix" % "3.3.0"
    val sttp                = "com.softwaremill.sttp.client3" %% "core"                      % Version.sttp
    val sttpBackendOkHttp   = "com.softwaremill.sttp.client3" %% "okhttp-backend"            % Version.sttp
    val airframeLog = "org.wvlet.airframe" %% "airframe-log" % Version.airframeLog
    val wireMock = "com.github.tomakehurst" % "wiremock" % "2.27.2"
    val mockServer = "org.mock-server" % "mockserver-netty" % "5.11.2"
    val mockServerClient = "org.mock-server" % "mockserver-client-java" % "5.11.2"
    val randomDataGen = "com.danielasfregola" %% "random-data-generator" % "2.9"
    val scalaFaker = "io.github.etspaceman" %% "scalacheck-faker" % "6.0.0"

    val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.3",
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
    resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven",
      "ksqlDb" at "https://ksqldb-maven.s3.amazonaws.com/maven",
      "confluentJenkins" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200715032424/1/maven/",
      "confluentJenkins2" at "https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/6.1.0-beta200916191548/1/maven/",
      Resolver.sonatypeRepo("releases"),
      Resolver.bintrayRepo("wolfendale", "maven"),
      Resolver.mavenLocal
    ),
    scalafmtOnCompile := true,
  )
