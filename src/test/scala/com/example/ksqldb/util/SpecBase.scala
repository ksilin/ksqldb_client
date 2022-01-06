package com.example.ksqldb.util

import com.example.ksqldb.FutureConverter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.util.Properties

class SpecBase
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport {

  EnvVarUtil.setEnv("RANDOM_DATA_GENERATOR_SEED", "9153932137467828920")
  Logger.setDefaultLogLevel(LogLevel.DEBUG)
  val loglevelProps = new Properties()
  loglevelProps.setProperty("org.apache.kafka", LogLevel.WARN.name)
  //Logger.setLogLevels(loglevelProps)

}
