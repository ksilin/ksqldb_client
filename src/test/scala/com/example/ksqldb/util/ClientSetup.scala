package com.example.ksqldb.util

import org.apache.kafka.clients.admin.AdminClient

import java.net.URL
import java.util.Properties

case class ClientSetup(configFileUrl: Option[URL] = None, configPath: Option[String] = None) {

  val clientProps = KsqlClientProps.ksqlClientPropsFromConfigFile(configFileUrl, configPath)
  private val props            = CCloudClientProps.create(configFileUrl, configPath)
  val commonProps: Properties  = props.clientProps
  val adminClient: AdminClient = AdminClient.create(commonProps)
}
