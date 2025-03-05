package org.conduktorassignmentsetup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentSetup.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC = "people-topic";

    public static void main(String[] args) {
        LOGGER.info("Starting MockPeopleProducer");

        PeopleTopicCreator peopleTopicCreator = new PeopleTopicCreator();
        peopleTopicCreator.createTopic();

        PeopleProducer peopleProducer = new PeopleProducer();
        Integer uploadedCount = peopleProducer.loadJsonAndSend();

        LOGGER.info("Uploaded {} people to Topic {}. Exiting.", uploadedCount, TOPIC);

    }

}