## Candidate: Dursun Satiroglu

### Description

A small application that, given a JSON (random-people-data.json), creates a Kafka topic named "people-topic" with 3 partitions. It will subsequently create a producer and upload all data within the JSON, with each node as a single record.

### Usage
#### Build and testing:

    ./gradlew clean build

#### Running the app:

    ./gradlew clean build run