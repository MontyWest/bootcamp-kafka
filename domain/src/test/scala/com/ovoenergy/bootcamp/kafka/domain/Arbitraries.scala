package com.ovoenergy.bootcamp.kafka.domain

import java.time.{Instant, LocalDateTime, Month, YearMonth}

import com.ovoenergy.bootcamp.kafka.domain.Account.AccountId
import com.ovoenergy.bootcamp.kafka.domain.Acquisition.AcquisitionId
import com.ovoenergy.bootcamp.kafka.domain.Customer.CustomerId
import com.ovoenergy.bootcamp.kafka.domain.PostalAddress.{AddressLine, City, PostCode}
import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import org.scalatest.time.Day

trait Arbitraries {

  implicit lazy val arbLocalDateTime: Arbitrary[LocalDateTime] = Arbitrary(
    for {
      year <- choose(1970, 2025)
      month <- oneOf(Month.values())
      day <- choose(1, YearMonth.of(year, month).lengthOfMonth() - 1)
      hour <- choose(0, 23)
      minutes <- choose(0, 59)
      seconds <- choose(0, 59)
    } yield LocalDateTime.of(year, month, day, hour, minutes, seconds)

  )

  implicit lazy val arbString: Arbitrary[String] = Arbitrary(
    listOfN(15, alphaNumChar).map(xs => xs.mkString)
  )

  implicit lazy val arbTariff: Arbitrary[Tariff] = Arbitrary(oneOf("Fixed", "Green", "Variable").map(Tariff.apply))

  implicit lazy val arbEmailAddress: Arbitrary[EmailAddress] = Arbitrary(
    Gen.oneOf("email@gmail.com", "email1@gmal.com").map(EmailAddress.unsafeFromString)
  )

  implicit lazy val arbCustomerId: Arbitrary[CustomerId] = Arbitrary(Gen.delay(const(CustomerId.unique())))

  implicit lazy val arbAccountId: Arbitrary[AccountId] = Arbitrary(Gen.delay(const(AccountId.unique())))

  implicit lazy val arbAcquisitionId: Arbitrary[AcquisitionId] = Arbitrary(Gen.delay(const(AcquisitionId.unique())))

  implicit lazy val arbAddressLine: Arbitrary[AddressLine] = Arbitrary(arbString.arbitrary.map(AddressLine.apply))

  implicit lazy val arbPostCode: Arbitrary[PostCode] = Arbitrary(
    listOfN(6, alphaNumChar).map(xs => xs.mkString).map(PostCode.apply)
  )

  implicit lazy val arbCity: Arbitrary[City] = Arbitrary(
    alphaStr.map(City.apply)
  )

  implicit lazy val arbPostalAddress: Arbitrary[PostalAddress] = Arbitrary(
    for {
      line1 <- arbitrary[AddressLine]
      line2 <- arbitrary[Option[AddressLine]]
      postCode <- arbitrary[PostCode]
      city <- arbitrary[City]
    } yield PostalAddress(line1, line2, postCode, city)
  )

  implicit lazy val arbAccount: Arbitrary[Account] = Arbitrary(for{
    id <- arbitrary[AccountId]
    acquisitionId <- arbitrary[AcquisitionId]
    tariff <- arbitrary[Tariff]
    domicileAddress <- arbitrary[PostalAddress]
    billingAddress <- arbitrary[PostalAddress]
  } yield Account(id, acquisitionId, tariff, domicileAddress, billingAddress))

  implicit lazy val arbCreateAccount: Arbitrary[CreateAccount] = Arbitrary(for{
    acquisitionId <- arbitrary[AcquisitionId]
    tariff <- arbitrary[Tariff]
    domicileAddress <- arbitrary[PostalAddress]
    billingAddress <- arbitrary[PostalAddress]
  } yield CreateAccount(acquisitionId, tariff, domicileAddress, billingAddress))

  implicit lazy val arbCustomer: Arbitrary[Customer] = Arbitrary(for{
    id <- arbitrary[CustomerId]
    acquisitionId <- arbitrary[AcquisitionId]
    name <- arbitrary[String]
    emailAddress <- arbitrary[EmailAddress]
    joinedOn <- arbitrary[LocalDateTime]
  } yield Customer(id, acquisitionId, name, emailAddress, joinedOn))

  implicit lazy val arbCreateCustomer: Arbitrary[CreateCustomer] = Arbitrary(for{
    acquisitionId <- arbitrary[AcquisitionId]
    name <- arbitrary[String]
    emailAddress <- arbitrary[EmailAddress]
  } yield CreateCustomer(acquisitionId, name, emailAddress))

  implicit lazy val arbAcquisition: Arbitrary[Acquisition] = Arbitrary(for{
    id <- arbitrary[AcquisitionId]
    customerName <- arbitrary[String]
    customerEmailAddress <- arbitrary[EmailAddress]
    tariff <- arbitrary[Tariff]
    domicileAddress <- arbitrary[PostalAddress]
    billingAddress <- arbitrary[PostalAddress]
  } yield Acquisition(id, customerName, customerEmailAddress, tariff, domicileAddress, billingAddress))

  implicit lazy val arbCreateAcquisition: Arbitrary[CreateAcquisition] = Arbitrary(for{
    customerName <- arbitrary[String]
    customerEmailAddress <- arbitrary[EmailAddress]
    tariff <- arbitrary[Tariff]
    domicileAddress <- arbitrary[PostalAddress]
    billingAddress <- arbitrary[PostalAddress]
  } yield CreateAcquisition(customerName, customerEmailAddress, tariff, domicileAddress, billingAddress))

}

object Arbitraries extends Arbitraries
