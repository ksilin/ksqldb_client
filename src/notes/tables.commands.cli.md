# short overview - know what happens when you create a table

## sql

## use CLI with docker

`> docker exec -it ksqldb-cli /bin/bash/`
`>> ksql http://ksqldb-server:8088`

or at once: 

`> docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`

## find describe and consume a table

```
ksql> DESCRIBE EXTENDED PRODUCTTABLE;

Name                 : PRODUCTTABLE
Type                 : TABLE
Timestamp field      : Not set - using <ROWTIME>
Key format           : KAFKA
Value format         : JSON
Kafka topic          : ksql_tst_topic-PRODUCTTABLE (partitions: 1, replication: 1)
Statement            : CREATE TABLE PRODUCTTABLE WITH (KAFKA_TOPIC='ksql_tst_topic-PRODUCTTABLE', PARTITIONS=1, REPLICAS=1) AS SELECT
  PRODUCTSTREAM.ID ID,
  COUNT_DISTINCT(PRODUCTSTREAM.DESCRIPTION) KSQL_COL_0
FROM PRODUCTSTREAM PRODUCTSTREAM
GROUP BY PRODUCTSTREAM.ID
EMIT CHANGES;

 Field      | Type                           
---------------------------------------------
 ID         | VARCHAR(STRING)  (primary key) 
 KSQL_COL_0 | BIGINT                         
---------------------------------------------

Queries that write from this TABLE
-----------------------------------
CTAS_PRODUCTTABLE_13 (RUNNING) : CREATE TABLE PRODUCTTABLE WITH (KAFKA_TOPIC='ksql_tst_topic-PRODUCTTABLE', PARTITIONS=1, REPLICAS=1) AS SELECT   PRODUCTSTREAM.ID ID,   COUNT_DISTINCT(PRODUCTSTREAM.DESCRIPTION) KSQL_COL_0 
FROM PRODUCTSTREAM PRODUCTSTREAM GROUP BY PRODUCTSTREAM.ID EMIT CHANGES;

For query topology and execution plan please run: EXPLAIN <QueryId>

Local runtime statistics
------------------------
messages-per-sec:         0   total-messages:         7     last-message: 2020-11-20T05:36:45.499Z

(Statistics of the local KSQL server interaction with the Kafka topic ksql_tst_topic-PRODUCTTABLE)

Consumer Groups summary:

Consumer Group       : _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13

Kafka topic          : products
Max lag              : 0

 Partition | Start Offset | End Offset | Offset | Lag 
------------------------------------------------------
 0         | 0            | 7          | 7      | 0   
------------------------------------------------------
```

* explain the query: CTAS_PRODUCTTABLE_13

```
ksql> explain CTAS_PRODUCTTABLE_13;

ID                   : CTAS_PRODUCTTABLE_13
Query Type           : PERSISTENT
SQL                  : CREATE TABLE PRODUCTTABLE WITH (KAFKA_TOPIC='ksql_tst_topic-PRODUCTTABLE', PARTITIONS=1, REPLICAS=1) AS SELECT
  PRODUCTSTREAM.ID ID,
  COUNT_DISTINCT(PRODUCTSTREAM.DESCRIPTION) KSQL_COL_0
FROM PRODUCTSTREAM PRODUCTSTREAM
GROUP BY PRODUCTSTREAM.ID
EMIT CHANGES;
Host Query Status    : {ksqldb-server:8088=RUNNING}

 Field      | Type                   
-------------------------------------
 ID         | VARCHAR(STRING)  (key) 
 KSQL_COL_0 | BIGINT                 
-------------------------------------

Sources that this query reads from: 
-----------------------------------
PRODUCTSTREAM

For source description please run: DESCRIBE [EXTENDED] <SourceId>

Sinks that this query writes to: 
-----------------------------------
PRODUCTTABLE

For sink description please run: DESCRIBE [EXTENDED] <SinkId>

Execution plan      
--------------      
 > [ SINK ] | Schema: ID STRING KEY, KSQL_COL_0 BIGINT | Logger: CTAS_PRODUCTTABLE_13.PRODUCTTABLE
		 > [ PROJECT ] | Schema: ID STRING KEY, KSQL_COL_0 BIGINT | Logger: CTAS_PRODUCTTABLE_13.Aggregate.Project
				 > [ AGGREGATE ] | Schema: ID STRING KEY, ID STRING, DESCRIPTION STRING, KSQL_AGG_VARIABLE_0 BIGINT | Logger: CTAS_PRODUCTTABLE_13.Aggregate.Aggregate
						 > [ GROUP_BY ] | Schema: ID STRING KEY, ID STRING, DESCRIPTION STRING | Logger: CTAS_PRODUCTTABLE_13.Aggregate.GroupBy
								 > [ PROJECT ] | Schema: ID STRING KEY, ID STRING, DESCRIPTION STRING | Logger: CTAS_PRODUCTTABLE_13.Aggregate.Prepare
										 > [ SOURCE ] | Schema: ID STRING KEY, DESCRIPTION STRING, ROWTIME BIGINT, ID STRING | Logger: CTAS_PRODUCTTABLE_13.KsqlTopic.Source


Processing topology 
------------------- 
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [products])
      --> KSTREAM-TRANSFORMVALUES-0000000001
    Processor: KSTREAM-TRANSFORMVALUES-0000000001 (stores: [])
      --> Aggregate-Prepare
      <-- KSTREAM-SOURCE-0000000000
    Processor: Aggregate-Prepare (stores: [])
      --> KSTREAM-AGGREGATE-0000000003
      <-- KSTREAM-TRANSFORMVALUES-0000000001
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [Aggregate-Aggregate-Materialize])
      --> Aggregate-Aggregate-ToOutputSchema
      <-- Aggregate-Prepare
    Processor: Aggregate-Aggregate-ToOutputSchema (stores: [])
      --> Aggregate-Project
      <-- KSTREAM-AGGREGATE-0000000003
    Processor: Aggregate-Project (stores: [])
      --> KTABLE-TOSTREAM-0000000006
      <-- Aggregate-Aggregate-ToOutputSchema
    Processor: KTABLE-TOSTREAM-0000000006 (stores: [])
      --> KSTREAM-SINK-0000000007
      <-- Aggregate-Project
    Sink: KSTREAM-SINK-0000000007 (topic: ksql_tst_topic-PRODUCTTABLE)
      <-- KTABLE-TOSTREAM-0000000006
```

* changelog topics for this query: _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13-Aggregate-Aggregate-Materialize-changelog

`./kafka-console-consumer --bootstrap-server localhost:29092 --topic _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13-Aggregate-Aggregate-Materialize-changelog`

=> seems to be empty, why?

* monitor the status of the consumer group

```
./kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13

GROUP                                                             TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                                                                                                                         HOST            CLIENT-ID
_confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13 products        0          7               7               0               _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13-d19920ab-77d2-4c85-badb-bd65241fc0fe-StreamThread-1-consumer-1898ee2c-7a45-463e-8efb-e5e589be4c8d /172.20.0.5     _confluent-ksql-ksql_tst_srv_ksql_presistent_CTAS_PRODUCTTABLE_13-d19920ab-77d2-4c85-badb-bd65241fc0fe-StreamThread-1-consumer
```

* print the sink topic: 

```
> print 'ksql_tst_topic-PRODUCTTABLE' from beginning;
Key format: KAFKA_STRING
Value format: JSON or KAFKA_STRING
rowtime: 2020/11/20 05:36:42.209 Z, key: strikelane, value: {"KSQL_COL_0":1}
rowtime: 2020/11/20 05:36:42.311 Z, key: bonanzi, value: {"KSQL_COL_0":1}
rowtime: 2020/11/20 05:36:42.413 Z, key: bankdust, value: {"KSQL_COL_0":1}
rowtime: 2020/11/20 05:36:42.516 Z, key: fashionergy, value: {"KSQL_COL_0":1}
rowtime: 2020/11/20 05:36:42.618 Z, key: bargainizer, value: {"KSQL_COL_0":1}
```

## describe and consume command topic

`./kafka-topics --bootstrap-server localhost:29092 --list`

`/kafka-console-consumer --bootstrap-server localhost:29092 --topic _confluent-ksql-ksql_tst_srv__command_topic --from-beginning`

example: 

```json
{
  "statement":"CREATE TABLE PRODUCTTABLE WITH (KAFKA_TOPIC='ksql_tst_topic-PRODUCTTABLE', PARTITIONS=1, REPLICAS=1) AS SELECT\n  PRODUCTSTREAM.ID ID,\n  COUNT_DISTINCT(PRODUCTSTREAM.DESCRIPTION) KSQL_COL_0\nFROM PRODUCTSTREAM PRODUCTSTREAM\nGROUP BY PRODUCTSTREAM.ID\nEMIT CHANGES;",
  "originalProperties":{
    "ksql.extension.dir":"ext",
    "ksql.streams.cache.max.bytes.buffering":"10000000",
    "ksql.transient.prefix":"transient_",
    "ksql.query.status.running.threshold.seconds":"300",
    "ksql.streams.default.deserialization.exception.handler":"io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler",
    "ksql.output.topic.name.prefix":"ksql_tst_topic-",
    "ksql.query.pull.enable.standby.reads":"false",
    "ksql.persistence.default.format.key":"KAFKA",
    "ksql.query.error.max.queue.size":"10",
    "ksql.internal.topic.min.insync.replicas":"1",
    "ksql.streams.shutdown.timeout.ms":"300000",
    "ksql.internal.topic.replicas":"1",
    "ksql.insert.into.values.enabled":"true",
    "ksql.key.format.enabled":"false",
    "ksql.query.pull.max.allowed.offset.lag":"9223372036854775807",
    "ksql.query.pull.max.qps":"2147483647",
    "ksql.access.validator.enable":"auto",
    "ksql.streams.bootstrap.servers":"broker:9092",
    "ksql.query.pull.metrics.enabled":"false",
    "ksql.create.or.replace.enabled":"true",
    "ksql.hidden.topics":"_confluent.*,__confluent.*,_schemas,__consumer_offsets,__transaction_state,connect-configs,connect-offsets,connect-status,connect-statuses",
    "ksql.cast.strings.preserve.nulls":"true",
    "ksql.authorization.cache.max.entries":"10000",
    "ksql.pull.queries.enable":"true",
    "ksql.suppress.enabled":"false",
    "ksql.sink.window.change.log.additional.retention":"1000000",
    "ksql.readonly.topics":"_confluent.*,__confluent.*,_schemas,__consumer_offsets,__transaction_state,connect-configs,connect-offsets,connect-status,connect-statuses",
    "ksql.query.persistent.active.limit":"2147483647",
    "ksql.authorization.cache.expiry.time.secs":"30",
    "ksql.query.retry.backoff.initial.ms":"15000",
    "ksql.streams.auto.offset.reset":"earliest",
    "ksql.connect.url":"http://localhost:8083",
    "ksql.service.id":"ksql_tst_srv_",
    "ksql.streams.default.production.exception.handler":"io.confluent.ksql.errors.ProductionExceptionHandlerUtil$LogAndFailProductionExceptionHandler",
    "ksql.streams.commit.interval.ms":"2000",
    "ksql.streams.topology.optimization":"all",
    "ksql.query.retry.backoff.max.ms":"900000",
    "ksql.streams.num.stream.threads":"4",
    "ksql.timestamp.throw.on.invalid":"false",
    "ksql.udfs.enabled":"true",
    "ksql.udf.enable.security.manager":"true",
    "ksql.streams.application.server":"http://ksqldb-server:8088",
    "ksql.udf.collect.metrics":"false",
    "ksql.persistent.prefix":"ksql_presistent_",
    "ksql.suppress.buffer.size.bytes":"-1"
  },
  "plan":{
    "@type":"ksqlPlanV1",
    "statementText":"CREATE TABLE PRODUCTTABLE WITH (KAFKA_TOPIC='ksql_tst_topic-PRODUCTTABLE', PARTITIONS=1, REPLICAS=1) AS SELECT\n  PRODUCTSTREAM.ID ID,\n  COUNT_DISTINCT(PRODUCTSTREAM.DESCRIPTION) KSQL_COL_0\nFROM PRODUCTSTREAM PRODUCTSTREAM\nGROUP BY PRODUCTSTREAM.ID\nEMIT CHANGES;",
    "ddlCommand":{
      "@type":"createTableV1",
      "sourceName":"PRODUCTTABLE",
      "schema":"`ID` STRING KEY, `KSQL_COL_0` BIGINT",
      "topicName":"ksql_tst_topic-PRODUCTTABLE",
      "formats":{
        "keyFormat":{
          "format":"KAFKA"
        },
        "valueFormat":{
          "format":"JSON"
        }
      },
      "orReplace":false
    },
    "queryPlan":{
      "sources":[
        "PRODUCTSTREAM"
      ],
      "sink":"PRODUCTTABLE",
      "physicalPlan":{
        "@type":"tableSinkV1",
        "properties":{
          "queryContext":"PRODUCTTABLE"
        },
        "source":{
          "@type":"tableSelectV1",
          "properties":{
            "queryContext":"Aggregate/Project"
          },
          "source":{
            "@type":"streamAggregateV1",
            "properties":{
              "queryContext":"Aggregate/Aggregate"
            },
            "source":{
              "@type":"streamGroupByKeyV1",
              "properties":{
                "queryContext":"Aggregate/GroupBy"
              },
              "source":{
                "@type":"streamSelectV1",
                "properties":{
                  "queryContext":"Aggregate/Prepare"
                },
                "source":{
                  "@type":"streamSourceV1",
                  "properties":{
                    "queryContext":"KsqlTopic/Source"
                  },
                  "topicName":"products",
                  "formats":{
                    "keyFormat":{
                      "format":"KAFKA"
                    },
                    "valueFormat":{
                      "format":"JSON"
                    }
                  },
                  "sourceSchema":"`ID` STRING KEY, `DESCRIPTION` STRING"
                },
                "keyColumnNames":[
                  "ID"
                ],
                "selectExpressions":[
                  "ID AS ID",
                  "DESCRIPTION AS DESCRIPTION"
                ]
              },
              "internalFormats":{
                "keyFormat":{
                  "format":"KAFKA"
                },
                "valueFormat":{
                  "format":"JSON"
                }
              }
            },
            "internalFormats":{
              "keyFormat":{
                "format":"KAFKA"
              },
              "valueFormat":{
                "format":"JSON"
              }
            },
            "nonAggregateColumns":[
              "ID",
              "DESCRIPTION"
            ],
            "aggregationFunctions":[
              "COUNT_DISTINCT(DESCRIPTION)"
            ]
          },
          "keyColumnNames":[
            "ID"
          ],
          "selectExpressions":[
            "KSQL_AGG_VARIABLE_0 AS KSQL_COL_0"
          ]
        },
        "formats":{
          "keyFormat":{
            "format":"KAFKA"
          },
          "valueFormat":{
            "format":"JSON"
          }
        },
        "topicName":"ksql_tst_topic-PRODUCTTABLE"
      },
      "queryId":"CTAS_PRODUCTTABLE_13"
    }
  },
  "version":1
}
```

##  

