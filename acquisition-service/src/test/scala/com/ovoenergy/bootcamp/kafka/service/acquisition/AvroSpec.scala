package com.ovoenergy.bootcamp.kafka.service.acquisition

import com.ovoenergy.bootcamp.kafka.domain.Acquisition
import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

class AvroSpec extends WordSpec with Matchers {

  "Avro" should {
    "generate a schema" in {
      val schema: Schema = AvroSchema[Acquisition]
      println(schema.toString(true))
      schema.toString.length should be > 0
    }
  }

}
