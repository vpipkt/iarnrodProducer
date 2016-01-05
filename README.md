# IarnrodProducer

A Kafka producer for a Geomesa DataStore based on Irish Rail (Iarnród Éirann) [realtime train status](http://api.irishrail.ie/realtime/)

IarnrodProducer is a small java application which gets the realtime status of Irish Rail trains every 30 seconds and publishes them to a user-configurable Kafka broker. The messages are GeoMesa simple features. The topic is named `geomesa-ds-kafka-iarnrod`.

## Build

`mvn clean install`

# Run

Run the application with three parameters:

* brokers - your Kafka broker instances, comma separated
* zookeepers - your Zookeeper nodes, comma separated
* zkPath - (optional) Zookeeper's path for metadata

Example: 

`java  -cp ./target/iarnrodProducer-{version}.jar iarnrodProducer -brokers localhost:9092 -zookeepers localhost:2181`

## Consume

Geoserver can consume the topic if the GeoMesa plugins are correctly installed as described in the [GeoMesa Deployment Tutorial](http://www.geomesa.org/geomesa-deployment/). The general outline of the [GeoMesa Kafka Quickstart](http://www.geomesa.org/geomesa-kafka-quickstart/) should be useful to consume the Irish Rail messages.

