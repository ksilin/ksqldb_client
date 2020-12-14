package com.example.ksqldb

import com.danielasfregola.randomdatagenerator.RandomDataGenerator
import org.scalacheck._
import faker._
// Import the default resource-loader implicits
import faker.ResourceLoader.Implicits._

object TestData extends RandomDataGenerator {

  case class SensorData(id: String, area: String, reading: Int)

  implicit val genSensorData: Arbitrary[SensorData] = Arbitrary {
    for {
      name    <- Gen.oneOf("car_1", "car_2", "car_3", "car_4")
      area    <- Gen.oneOf("wheels", "engine", "trunk", "interior")
      reading <- Gen.choose(0, 100)
    } yield SensorData(name, area, reading)
  }

  case class Address(street: String, building: String, index: String)

  implicit val genAddress: Arbitrary[Address] = Arbitrary {
    for {
      street   <- Arbitrary.arbitrary[address.StreetName]
      building <- Arbitrary.arbitrary[address.BuildingNumber]
      index    <- Arbitrary.arbitrary[address.PostalCode]
    } yield Address(street.value, building.value, index.value)
  }

  val userIds: Seq[String] = (1 to 10).map(_.toString)

  case class User(id: String, name: String, address: Address, changedAt: Long)

  implicit val genUser: Arbitrary[User] = Arbitrary {
    for {
      id      <- Gen.oneOf(userIds)
      name    <- Arbitrary.arbitrary[name.FullName]
      address <- genAddress.arbitrary
    } yield User(id, name.value, address, System.currentTimeMillis())
  }

  val prodIds = List(
    "bargainizer",
    "strikelane",
    "bankdust",
    "fashionergy",
    "hyperfreeze",
    "mysteryessence",
    "bonanzi",
    "promogalore"
  )

  case class Product(id: String, description: String)

  implicit val genProduct: Arbitrary[Product] = Arbitrary {
    for {
      prodId      <- Gen.oneOf(prodIds)
      description <- Arbitrary.arbitrary[pokemon.PokemonName]
    } yield Product(prodId, description.toString)
  }

  case class Price(prodId: String, price: Int)

  case class PricePlacement(
      id: String,
      location: String,
      description: String,
      startTime: Long,
      endTime: Long
  )

  case class Order(
      id: String,
      userId: String,
      prodId: String,
      amount: Int,
      location: String,
      timestamp: Long
  )

  implicit val genOrder: Arbitrary[Order] = Arbitrary {
    for {
      id       <- Gen.uuid
      userId   <- Gen.oneOf(userIds)
      prodId   <- Gen.oneOf(prodIds)
      amount   <- Gen.chooseNum(1, 10)
      location <- Arbitrary.arbitrary[address.Country]
    } yield Order(id.toString, userId, prodId, amount, location.name, System.currentTimeMillis())
  }

  case class Shipment(id: String, orderId: String, warehouse: String, timestamp: Long)

  val warehouses = List("NORTH", "SOUTH", "WEST", "EAST", "CENTER")

  def makeGenShipment(orderIds: Iterable[String]): Arbitrary[Shipment] = Arbitrary {
    for {
      id        <- Gen.uuid
      orderId   <- Gen.oneOf(orderIds)
      warehouse <- Gen.oneOf(warehouses)
    } yield Shipment(id.toString, orderId, warehouse, System.currentTimeMillis())
  }

  case class Click(userId: String, element: String, userAgent: String, timestamp: Long)

  implicit val genClick: Arbitrary[Click] = Arbitrary {
    for {
      userId          <- Gen.oneOf(userIds)
      element         <- Arbitrary.arbitrary[internet.Slug]
      userAgent       <- Arbitrary.arbitrary[internet.UserAgent]
      timestampOffset <- Gen.chooseNum(0, 100000)
    } yield Click(
      userId,
      element.value,
      userAgent.value,
      System.currentTimeMillis() + timestampOffset
    )

  }

}
