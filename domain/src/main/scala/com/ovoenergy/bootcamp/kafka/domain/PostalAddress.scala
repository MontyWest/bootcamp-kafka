package com.ovoenergy.bootcamp.kafka.domain

import com.ovoenergy.bootcamp.kafka.domain.PostalAddress.{AddressLine, City, PostCode}

case class PostalAddress(lineOne: AddressLine, lineTwo: Option[AddressLine], postCode: PostCode, city: City)

object PostalAddress {

  case class AddressLine(value: String)

  case class PostCode(value: String)

  case class City(value: String)

}