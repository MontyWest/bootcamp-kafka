package com.ovoenergy.bootcamp.kafka.domain

import java.net.InetAddress
import java.time.Instant

import com.ovoenergy.bootcamp.kafka.domain.Acquisition.AcquisitionId

import scala.util.hashing.MurmurHash3

case class Acquisition(id: AcquisitionId,
                       customerName: String,
                       customerEmailAddress: EmailAddress,
                       tariff: Tariff,
                       domicileAddress: PostalAddress,
                       billingAddress: PostalAddress)

object Acquisition {

  case class AcquisitionId(value: String)

  object AcquisitionId {

    def unique(): AcquisitionId = {
      val hash = math.abs(MurmurHash3.productHash(("AcquisitionId", InetAddress.getLocalHost, Instant.now())))
      AcquisitionId(s"acq-$hash")
    }
  }


}