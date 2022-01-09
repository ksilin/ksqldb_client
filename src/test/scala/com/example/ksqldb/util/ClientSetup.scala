package com.example.ksqldb.util

import io.confluent.ksql.api.client.Client
import org.apache.kafka.clients.admin.AdminClient

import java.net.URL
import java.util.Properties

case class ClientSetup(configFileUrl: Option[URL] = None, configPath: Option[String] = None) {

  val client: Client           = KsqlClientProps.ksqlClientFromConfig(configFileUrl, configPath)
  private val props            = CCloudClientProps.create(configFileUrl, configPath)
  val commonProps: Properties  = props.clientProps
  val adminClient: AdminClient = AdminClient.create(commonProps)
}
