package com.example.ksqldb

object CsvTypes {

  case class RiderLocation(profileId: String, latitude: Double, longitude: Double, timestamp: Long)
}
