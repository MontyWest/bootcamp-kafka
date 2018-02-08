package com.ovoenergy.bootcamp.kafka.service.account

case class Settings(httpHost: String, httpPort: Int,
  kafkaEndpoint: String,
  schemaRegistryEndpoint: String)
