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
import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._

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
  val ksqlClient: Client       = ksqlClientFromKslqClientProps(setup.ksqlClientProps)
  val adminClient: AdminClient = setup.adminClient

  private val bootstrapServer: String =
    setup.commonProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).asInstanceOf[String]
  // 1 for local deployments, 3 for ccloud
  val replicationFactor: Short = if (bootstrapServer.contains("cloud")) 3 else 1
  val pollTimeout: Duration    = Duration.ofMillis(1000)

  // not clear why ccloud accepts the deprecated version and explodes on the new one
  // https://confluent.slack.com/archives/C337JP2F8/p1662450431689659
  //val queryProperties: util.Map[String, Object] = Map[String, Object]("auto.offset.reset" -> "earliest", "statestore.cache.max.bytes" -> "0").asJava
  val queryProperties: util.Map[String, Object] = Map[String, Object]("auto.offset.reset" -> "earliest", "cache.max.bytes.buffering" -> "0").asJava

  def ksqlClientFromKslqClientProps(props: KsqlClientProps): Client = {

    val clientOptions: ClientOptions = {
      val opt: ClientOptions = ClientOptions
        .create()
        .setHost(props.host)
        .setPort(props.port)

      if (props.port == 443) opt.setUseTls(true)

      props.apiKey.foreach { k =>
        props.apiSecret.foreach { s =>
          opt.setBasicAuthCredentials(k, s)
        }
      }
      opt
    }

    props.apiKey.zip(props.apiSecret) foreach { case (key, secret) =>
      clientOptions.setBasicAuthCredentials(key, secret)
      clientOptions.setUseTls(true)
      clientOptions.setUseAlpn(true)
    }
    Client.create(clientOptions)
  }

}
