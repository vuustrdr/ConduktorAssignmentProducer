package org.conduktorassignmentsetup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.conduktorassignmentsetup.AssignmentSetup.BOOTSTRAP_SERVERS;
import static org.conduktorassignmentsetup.AssignmentSetup.TOPIC;

public class PeopleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeopleProducer.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;


    public PeopleProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
    }

    public Integer loadJsonAndSend() {

        LOGGER.info("Loading JSON file");
        JsonNode peopleNode;

        try {

            JsonNode node = objectMapper.readTree(new File("/Users/dursun/IdeaProjects/MockPeopleProducer/src/main/resources/random-people-data.json"));
            peopleNode = node.get("ctRoot");

        } catch (IOException e) {
            LOGGER.error("Load Json failed due to {}", e.getMessage());
            return 0;
        }

        final Integer[] uploadCount = {0};

        for (JsonNode personNode : peopleNode) {

            String person;

            try {
                person = objectMapper.writeValueAsString(personNode);
            } catch (Exception e){
                LOGGER.error("Json Obj could not be mapped to String. Error: {}", e.getMessage());
                continue;
            }

            producer.send(new ProducerRecord<>(TOPIC, personNode.get("_id").asText(), person), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                        uploadCount[0]++;
                    } else {
                        LOGGER.error("Error while producing", e);
                    }
                }
            });

        }

        producer.flush();
        producer.close();

        return uploadCount[0];
    }
}
