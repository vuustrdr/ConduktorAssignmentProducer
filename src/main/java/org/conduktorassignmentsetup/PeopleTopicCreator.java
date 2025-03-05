package org.conduktorassignmentsetup;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import java.util.Properties;

import static org.conduktorassignmentsetup.AssignmentSetup.BOOTSTRAP_SERVERS;
import static org.conduktorassignmentsetup.AssignmentSetup.TOPIC;

public class PeopleTopicCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeopleTopicCreator.class);

    private static final int PARTITIONS = 3;

    private Properties props;

    public PeopleTopicCreator() {
        props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    public void createTopic() {

        try (AdminClient client = AdminClient.create(props)) {

            LOGGER.info("Deleting any existing Topics with name: {}", TOPIC);
            client.deleteTopics(Collections.singleton(TOPIC)).all().get();

            Thread.sleep(5000);

            LOGGER.info("Creating new Topic: {}", TOPIC);
            NewTopic newTopic = new NewTopic(TOPIC, PARTITIONS, (short) 1);
            client.createTopics(Collections.singletonList(newTopic)).all().get();

            Thread.sleep(5000);

            LOGGER.info("Topic '{}' created with 3 partitions", TOPIC);

        } catch (Exception e) {
            LOGGER.error("Failed to create Topic: '{}', {}", TOPIC, e.getMessage());
        }
    }

}
