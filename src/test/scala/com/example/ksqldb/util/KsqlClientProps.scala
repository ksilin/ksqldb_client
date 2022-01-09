package com.example.ksqldb.util

import com.typesafe.config.{ Config, ConfigFactory }
import io.confluent.ksql.api.client.{ Client, ClientOptions }

import java.net.URL

case class KsqlClientProps(
    host: String,
    port: Int,
    apiKey: Option[String],
    apiSecret: Option[String]
)

case object KsqlClientProps {

  val prefix = "ksql"

  def ksqlClientFromConfig(
      configFileUrl: Option[URL] = None,
      configPath: Option[String] = None
  ): Client = {
    val topLevelConfig = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    val config         = configPath.map(path => topLevelConfig.getConfig(path)).getOrElse(topLevelConfig)
    ksqlClientFromConfig(config)
  }

  def ksqlClientFromConfig(config: Config): Client = {
    val props = KsqlClientProps(
      config.getString(s"${prefix}.host"),
      config.getInt(s"${prefix}.port"),
      if (config.hasPath(s"${prefix}.key")) Some(config.getString(s"${prefix}.key")) else None,
      if (config.hasPath(s"${prefix}.secret")) Some(config.getString(s"${prefix}.secret")) else None
    )
    ksqlClientFromKslqClientProps(props)
  }

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
