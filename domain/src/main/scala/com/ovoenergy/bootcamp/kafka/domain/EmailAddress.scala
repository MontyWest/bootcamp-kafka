package com.ovoenergy.bootcamp.kafka.domain

case class EmailAddress(value: String)

object EmailAddress {

  // It is a very trivial one
  private val EmailRegex = """^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$""".r

  def fromString(str: String): Either[String, EmailAddress] = str match {
    case EmailRegex(foo) => Right(EmailAddress(str))
    case _ => Left(s"$str is not a valid email address")
  }

  def unsafeFromString(str: String): EmailAddress = fromString(str).right.get

}
