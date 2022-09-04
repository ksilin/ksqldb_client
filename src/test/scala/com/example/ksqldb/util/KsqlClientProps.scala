package com.example.ksqldb.util

import com.typesafe.config.{ Config, ConfigFactory }

import java.net.URL

case class KsqlClientProps(
    host: String,
    port: Int,
    apiKey: Option[String] = None,
    apiSecret: Option[String] = None
)

case object KsqlClientProps {

  val prefix = "ksql"

  def ksqlClientPropsFromConfigFile(
      configFileUrl: Option[URL] = None,
      configPath: Option[String] = None
  ): KsqlClientProps = {
    val topLevelConfig = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    val config         = configPath.map(path => topLevelConfig.getConfig(path)).getOrElse(topLevelConfig)
    ksqlClientPropsFromConfig(config)
  }

  def ksqlClientPropsFromConfig(config: Config): KsqlClientProps =
    KsqlClientProps(
      config.getString(s"${prefix}.host"),
      config.getInt(s"${prefix}.port"),
      if (config.hasPath(s"${prefix}.key")) Some(config.getString(s"${prefix}.key")) else None,
      if (config.hasPath(s"${prefix}.secret")) Some(config.getString(s"${prefix}.secret")) else None
    )

}
