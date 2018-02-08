package com.ovoenergy.bootcamp.kafka.service.customer

case class Settings(httpHost: String,
  httpPort: Int,
  kafkaEndpoint: String,
  schemaRegistryEndpoint: String)
