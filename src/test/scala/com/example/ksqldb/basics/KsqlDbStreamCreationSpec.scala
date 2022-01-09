package com.example.ksqldb.basics

import com.example.ksqldb.util._
import io.confluent.ksql.api.client.{ StreamInfo, TopicInfo }

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KsqlDbStreamCreationSpec extends SpecBase(configPath = Some("ccloud.stag.local")) {

  // stream can be simply deleted without stopping it
  // streams created as SELECT must be stopped before removal
  // remove stream if exists
  val streamName = "testStream"
  val tableName  = "testTable"

  KsqlSpecHelper.deleteTable(tableName, ksqlClient)

  // setting auto.offset.reset here does not work:
  // io.confluent.ksql.api.client.exception.KsqlClientException: The ksqlDB server accepted the statement issued via executeStatement(), but the response received is of an unexpected format.
  // The executeStatement() method is only for 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS SELECT' statements.
  // val autoOffsetResetEarliest = "SET 'auto.offset.reset' = 'earliest';"
  // val res: ExecuteStatementResult = client.executeStatement(autoOffsetResetEarliest).get()
  // println(s"setting auto.offset.reset to earliest: $res")

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

  "create stream for topic which does not exist" in {

    KsqlSpecHelper.deleteStream(streamName, ksqlClient, adminClient)
    val sinkTopicName = "DOES_NOT_EXIST"

    val topicsBefore: mutable.Buffer[TopicInfo] = ksqlClient.listTopics().get.asScala
    topicsBefore.find(_.getName.equalsIgnoreCase(sinkTopicName)) mustBe empty

    val streamsBefore: mutable.Buffer[StreamInfo] = ksqlClient.listStreams().get().asScala
    streamsBefore.find(_.getName.equalsIgnoreCase(streamName)) mustBe empty

    val createStreamStatement =
      s"CREATE OR REPLACE STREAM $streamName (id VARCHAR KEY) WITH (kafka_topic='$sinkTopicName', value_format='json', partitions=1);"
    ksqlClient.executeStatement(createStreamStatement).get()

    // must have the stream created
    val streamsAfter: mutable.Buffer[StreamInfo] = ksqlClient.listStreams().get().asScala
    streamsAfter.find(_.getName.equalsIgnoreCase(streamName)) mustNot be(empty)

    // and the topic too
    val topicsAfter: mutable.Buffer[TopicInfo] = ksqlClient.listTopics().get.asScala
    topicsAfter.find(_.getName.equalsIgnoreCase(sinkTopicName)) mustNot be(empty)
  }

  "create stream for a topic that exists but with non-matching schema - ksqlDB does not care" in {

    KsqlSpecHelper.deleteStream(streamName, ksqlClient, adminClient, false)

    val existingTopicName = "default_ksql_processing_log"

    val streamsBefore: mutable.Buffer[StreamInfo] = ksqlClient.listStreams().get().asScala
    streamsBefore.find(_.getName.equalsIgnoreCase(streamName)) mustBe empty

    val createStreamStatement =
      s"CREATE OR REPLACE STREAM $streamName (id VARCHAR KEY) WITH (kafka_topic='$existingTopicName', value_format='json', partitions=1);"
    ksqlClient.executeStatement(createStreamStatement).get()

    // must have the stream created
    val streamsAfter: mutable.Buffer[StreamInfo] = ksqlClient.listStreams().get().asScala
    streamsAfter.find(_.getName.equalsIgnoreCase(streamName)) mustNot be(empty)
  }

}
