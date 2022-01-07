package com.example.ksqldb.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }

import java.net.URL
import java.util.Properties

case class CCloudClientProps(
                               bootstrapServer: String,
                               apiKey: Option[String],
                               apiSecret: Option[String]
                             ) extends ClientProps {

  val clientProps: Properties = new Properties()
  clientProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)

  apiKey.foreach  { k =>
    apiSecret.foreach { s =>

      clientProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
      clientProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
      clientProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")

      val saslString: String =
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${k}" password="${s}";""".stripMargin
      clientProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslString)
    }
  }
}

case object CCloudClientProps {
  def create(configFileUrl: Option[URL] = None, configPath: Option[String] = None): CCloudClientProps = {
    val topLevelConfig = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    val config =  configPath.map(path => topLevelConfig.getConfig(path)).getOrElse(topLevelConfig)
    CCloudClientProps(
      config.getString("kafka.bootstrap"),
      if (config.hasPath("kafka.key")) Some(config.getString("kafka.key")) else None,
      if (config.hasPath("kafka.secret")) Some(config.getString("kafka.secret")) else None
    )
  }
}
