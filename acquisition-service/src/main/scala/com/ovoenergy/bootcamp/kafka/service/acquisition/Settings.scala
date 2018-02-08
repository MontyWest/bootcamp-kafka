package com.ovoenergy.bootcamp.kafka.service.acquisition

case class Settings(httpHost: String,
                    httpPort: Int,
                    kafkaEndpoint: String,
                    schemaRegistryEndpoint: String)
