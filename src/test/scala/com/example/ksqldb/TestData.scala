package com.example.ksqldb

import com.danielasfregola.randomdatagenerator.RandomDataGenerator
import org.scalacheck._

object TestData extends RandomDataGenerator {

  case class SensorData(id: String, area: String, reading: Int)

  implicit val genSensorData: Arbitrary[SensorData] = Arbitrary {
    for {
      name <- Gen.oneOf("car_1", "car_2", "car_3", "car_4")
      area <- Gen.oneOf("wheels", "engine", "trunk", "interior")
      reading <- Gen.choose(0, 100)
    } yield SensorData(name, area, reading)
  }

}
