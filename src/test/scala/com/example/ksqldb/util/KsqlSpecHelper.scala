package com.example.ksqldb.util

import io.confluent.ksql.api.client._
import monix.execution.Ack
import monix.execution.Ack.Continue
import monix.reactive.Observer
import org.apache.kafka.clients.admin.{ AdminClient, CreateTopicsResult, NewTopic }
import org.apache.kafka.common.config.TopicConfig
import wvlet.log.LogSupport

import java.io.PrintStream
import java.util.Collections
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object KsqlSpecHelper extends LogSupport {

  def prepareTest(
      streamsToDelete: List[String] = Nil,
      tablesToDelete: List[String] = Nil,
      topicsToDelete: List[String] = Nil,
      topicsToCreate: List[String] = Nil,
      client: Client,
      adminClient: AdminClient,
      replicationFactor: Short,
      numberOfPartitions: Int = 1
  ): Unit = {
    streamsToDelete foreach { s => deleteStream(s, client, adminClient) }
    tablesToDelete foreach { t => deleteTable(t, client) }
    topicsToDelete ++ tablesToDelete.map(_.toUpperCase) foreach { t => deleteTopic(t, adminClient) }
    topicsToCreate foreach { t =>
      createTopic(t, adminClient, replicationFactor, numberOfPartitions)
    }
  }

  def createTopic(
      topicName: String,
      adminClient: AdminClient,
      replicationFactor: Short,
      numberOfPartitions: Int = 1
  ): Unit =
    if (!adminClient.listTopics().names().get().contains(topicName)) {
      debug(s"Creating topic ${topicName}")

      val configs: Map[String, String] =
        if (replicationFactor < 3) Map(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "1")
        else Map.empty

      val newTopic: NewTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor)
      newTopic.configs(configs.asJava)
      try {
        val topicsCreationResult: CreateTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic))
        topicsCreationResult.all().get()
      } catch {
        case e: Throwable => info(e)
      }
    } else {
      info(s"topic $topicName already exists, skipping")
    }

  def deleteTopic(topicName: String, adminClient: AdminClient): Any = {
    info(s"deleting topic $topicName")
    try {
      val topicDeletionResult = adminClient.deleteTopics(List(topicName).asJava)
      topicDeletionResult.all().get()
    } catch {
      case e: Throwable => debug(e)
    }
  }

  // deletion is quite cumbersome as there is no good way to ID all dependencies
  // need to fetch SQL for all streams and tables and compare substrings to ID relevant ones
  def deleteStream(
      streamName: String,
      client: Client,
      adminClient: AdminClient,
      deleteSinkTopic: Boolean = true
  ): Unit = {
    val streams: mutable.Buffer[StreamInfo] = client.listStreams().get().asScala

    streams.find(_.getName.equalsIgnoreCase(streamName)).foreach { streamToDelete =>
      info(s"found existing stream: $streamToDelete")

      if (deleteSinkTopic) {
        val topicName: String = streamToDelete.getTopic
        info(s"deleting sink topic: $topicName")
        deleteTopic(topicName, adminClient)
      }

      var relevantQueries = getQueriesForSource(client, streamName)
      debug(s"queries for stream $streamName: ")
      relevantQueries.foreach(q => debug(q.getId))

      // terminate queries
      var retries = 0
      while (relevantQueries.nonEmpty && retries < 3) {

        val relevantQueryIds = relevantQueries.map(_.getId)

        relevantQueryIds.foreach { qId =>
          debug(s"terminating ${qId}")
          try {
            val res: ExecuteStatementResult =
              client.executeStatement(s"""TERMINATE "${qId}";""").get()
            info(res)
          } catch {
            case e: Throwable => debug(s"failed to terminate query ${qId}: $e")
          }
        }
        // TODO filter out the ones we failed on
        retries = retries + 1
        relevantQueries = getQueriesForSource(
          client,
          streamName
        ) //q.filter(_.getSql.toUpperCase.contains(streamName.toUpperCase))
      }
      deleteTablesForName(client, streamName)
      deleteStreamsForName(client, streamName)
    }
  }

  def getQueriesForSource(client: Client, sourceName: String): List[QueryInfo] = {
    val q = client.listQueries().get().asScala.toList
    q.filter(_.getSql.toUpperCase.contains(sourceName.toUpperCase))
  }

  def deleteStreamsForName(client: Client, name: String): List[ExecuteStatementResult] = {
    val streams     = client.listStreams().get.asScala.toList
    val streamNames = streams.map(_.getName)
    deleteSourceForName(client, "STREAM")(streamNames, name)
  }

  def deleteTablesForName(client: Client, name: String): List[ExecuteStatementResult] = {
    val tables     = client.listTables().get.asScala.toList
    val tableNames = tables.map(_.getName)
    deleteSourceForName(client, "TABLE")(tableNames, name)
  }

  private def deleteSourceForName(client: Client, kind: String)(sourceNames: List[String], name: String): List[ExecuteStatementResult] = {
    val sourceDescriptions: List[SourceDescription] =
      sourceNames.map(client.describeSource(_).get())
    val relevantSources: List[SourceDescription] = sourceDescriptions.filter { sd =>
      sd.sqlStatement().toUpperCase.contains(name.toUpperCase)
    }
    relevantSources map { s =>
      val sourceDeleted: ExecuteStatementResult =
        client.executeStatement(s"DROP ${kind.toUpperCase} IF EXISTS ${s.name()};").get()
      debug(s"$kind ${s.name()} deleted: $sourceDeleted")
      sourceDeleted
    }
  }

  def deleteTable(tableName: String, client: Client): Unit = {
    debug(s"deleting table $tableName")
    client.listTables().get.asScala.find(t => t.getName.equalsIgnoreCase(tableName)).foreach {
      t: TableInfo =>
        val tableDescription: SourceDescription = client.describeSource(t.getName).get
        val allQueries: mutable.Seq[QueryInfo] =
          tableDescription.readQueries().asScala ++ tableDescription.writeQueries().asScala
        allQueries foreach { q =>
          debug(s"terminating ${q.getId}")
          val res: ExecuteStatementResult = client.executeStatement(s"TERMINATE ${q.getId};").get()
          debug(res)
        }
        try client.executeStatement(s"DROP TABLE IF EXISTS $tableName;").get
        catch {
          case e: Throwable => info(s"failed to drop table $tableName: $e")
        }
    }
  }

  def printSourceDescription(sd: SourceDescription): Unit = {
    info("name: " + sd.name())
    info("type: " + sd.`type`())
    info("topic: " + sd.topic())
    info("keyFormat: " + sd.keyFormat())
    info("valueFormat: " + sd.valueFormat())
    info("timestampColumn: " + sd.timestampColumn())
    info("windowType: " + sd.windowType())
    info("fields: " + sd.fields())
    info("sqlStatement: " + sd.sqlStatement())
    info("readQueries: " + sd.readQueries())
    info("writeQueries: " + sd.writeQueries())
  }

  def makeRowObserver(
      prefix: String,
      out: PrintStream = System.out,
      nextPlugin: Option[(Row => Unit)] = None
  ): Observer[Row] =
    new Observer[Row] {

      var pos = 0

      def onNext(elem: Row): Ack = {
        out.println(s"$pos: $prefix ->${elem.columnNames()} : ${elem.values()}")
        pos += 1
        nextPlugin.foreach(f => f(elem))
        Continue
      }

      def onError(ex: Throwable): Unit = {
        out.println(s"$pos: $prefix -> $ex")
        pos += 1
      }

      def onComplete(): Unit = {
        out.println(s"$pos: $prefix completed")
        pos += 1
      }
    }

}
