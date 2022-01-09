package com.example.ksqldb.util

import io.confluent.ksql.api.client.{Client, ClientOptions}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.net.URL
import java.time.Duration
import java.util.Properties

class SpecBase(configFileUrl: Option[URL] = None, configPath: Option[String] = None)
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with LogSupport {

  EnvVarUtil.setEnv("RANDOM_DATA_GENERATOR_SEED", "9153932137467828920")
  Logger.setDefaultLogLevel(LogLevel.INFO)
  val loglevelProps = new Properties()
  loglevelProps.setProperty("org.apache.kafka", LogLevel.WARN.name)
  loglevelProps.setProperty("security", LogLevel.INFO.name)
  Logger.setLogLevels(loglevelProps)

  val setup: ClientSetup =
    ClientSetup(configFileUrl, configPath)
  val ksqlClient: Client       = ksqlClientFromKslqClientProps(setup.clientProps)
  val adminClient: AdminClient = setup.adminClient

  private val bootstrapServer: String =
    setup.commonProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).asInstanceOf[String]
  // 1 for local deployments, 3 for ccloud
  val replicationFactor: Short = if (bootstrapServer.contains("cloud")) 3 else 1
  val pollTimeout: Duration    = Duration.ofMillis(1000)

  def ksqlClientFromKslqClientProps(props: KsqlClientProps): Client = {

    val clientOptions: ClientOptions = ClientOptions
      .create()
      .setHost(props.host)
      .setPort(props.port)

    props.apiKey.zip(props.apiSecret) foreach { case (key, secret) =>
      clientOptions.setBasicAuthCredentials(key, secret)
      clientOptions.setUseTls(true)
      clientOptions.setUseAlpn(true)
    }
    Client.create(clientOptions)
  }

}
