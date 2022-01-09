package com.example.ksqldb.rest

import com.example.ksqldb.util._
import io.confluent.ksql.api.client.ServerInfo
import sttp.capabilities
import sttp.client3._
import sttp.client3.{ Identity, basicRequest }
import sttp.client3.okhttp.OkHttpSyncBackend

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KsqlDbRestApiSpec extends SpecBase(configPath = Some("ccloud.stag.local")) {

  val backend: SttpBackend[Identity, capabilities.WebSockets] = OkHttpSyncBackend()
  val host: String                                            = setup.clientProps.host
  val port: Int                                               = setup.clientProps.port

  // healthcheck response example:
  /* {
    "isHealthy": true,
    "details": {
      "metastore": {
      "isHealthy": true
    },
      "kafka": {
      "isHealthy": true
    },
          "commandRunner": {
      "isHealthy": true
    }
    }
  } */

  "healthcheck" in {
    val response: Identity[Response[Either[String, String]]] = basicRequest
      .get(uri"http://$host:$port/healthcheck")
      .send(backend)
    println("response:")
    println(response.body)
  }

  // Enable this endpoint by setting ksql.heartbeat.enable to true.
  // Optionally, you can also set ksql.lag.reporting.enable to true
  // ksqlDB servers in a cluster discover each other through persistent queries.
  // If you have no persistent queries running,
  // then the /clusterStatus endpoint contains info for the particular server that was queried,
  // rather than all servers in the cluster.
  // https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-rest-api/cluster-status-endpoint/

  // clsuter status response example:
  // {
  //  "clusterStatus": {
  //    "ksqldb-server:8088": {
  //      "hostAlive": true,
  //      "lastStatusUpdateMs": 1641743992816,
  //      "activeStandbyPerQuery": {},
  //      "hostStoreLags": {
  //        "stateStoreLags": {},
  //        "updateTimeMs": 1641744002923
  //      }
  //    }
  //  }
  //}

  "get cluster status" in {
    val response: Identity[Response[Either[String, String]]] = basicRequest
      .get(uri"http://$host:$port/clusterStatus")
      .send(backend)
    println("response:")
    println(response.body)
  }

  "exec a statement" in {}

  "run a query" in {}

  // using special methods in the client for these endpoints
  // see docs for more info: https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-rest-api/info-endpoint/
  "get server status and session variables" in {

    val serverInfo: ServerInfo = ksqlClient.serverInfo().get()
    info("serverInfo:")
    info(s"version: ${serverInfo.getServerVersion}")
    info(s"service ID: ${serverInfo.getKsqlServiceId}")
    info(s"kafka cluster ID: ${serverInfo.getKafkaClusterId}")

    val variables: mutable.Map[String, AnyRef] = ksqlClient.getVariables.asScala
    info(s"found ${variables.size} variables:")
    variables foreach println
  }

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

}
