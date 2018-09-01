package org.yanwang.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamsApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(XMLMessagesDispatcher.class);
    private static final String CONFIG_FILE = "config.properties";
    private static final Properties prop = new Properties();
    private static InputStream input = null;

    public static void main(String[] args) {

        try {

            Arrays.stream(args).forEach(
                    item -> {
                        if (item.contains(CONFIG_FILE)) {
                            try {
                                input = new FileInputStream(item);
                                prop.load(input);
                            } catch (FileNotFoundException e) {
                                LOGGER.error("Error happened at KafkaStreamsApp ", e);
                            } catch (IOException e) {
                                LOGGER.error("Error happened at KafkaStreamsApp ", e);
                            }
                        }
                    }
            );

            LOGGER.info("KAFKA_STREAMS_APPLICATION_ID:  " + prop.getProperty("KAFKA_STREAMS_APPLICATION_ID"));
            LOGGER.info("NUM_STREAM_THREADS_CONFIG: " + prop.getProperty("NUM_STREAM_THREADS_CONFIG"));
            LOGGER.info("BOOTSTRAPSERVER_CONFIG:    " + prop.getProperty("BOOTSTRAPSERVER_CONFIG"));
            
            LOGGER.info("FROM_TOPIC:    " + prop.getProperty("FROM_TOPIC"));
            LOGGER.info("TO_TOPIC:  " + prop.getProperty("TO_TOPIC"));
            LOGGER.info("XML_TAG_LOCALNAME:   " + prop.getProperty("XML_TAG_LOCALNAME"));
            LOGGER.info("MATCH_CONTENT: " + prop.getProperty("MATCH_CONTENT"));

            LOGGER.info("SECURITY_PROTOCOL_CONFIG:    " + prop.getProperty("SECURITY_PROTOCOL_CONFIG"));
            LOGGER.info("SASL_MECHANISM:    " + prop.getProperty("SASL_MECHANISM"));
            LOGGER.info("SSL_TRUSTSTORE_LOCATION_CONFIG:    " + prop.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG"));
            LOGGER.info("SSL_TRUSTSTORE_PASSWORD_CONFIG:    " + prop.getProperty("SSL_TRUSTSTORE_PASSWORD_CONFIG"));
            LOGGER.info("SSL_KEYSTORE_LOCATION_CONFIG:    " + prop.getProperty("SSL_KEYSTORE_LOCATION_CONFIG"));
            LOGGER.info("SSL_KEYSTORE_PASSWORD_CONFIG:    " + prop.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG"));
            LOGGER.info("SSL_KEY_PASSWORD_CONFIG:    " + prop.getProperty("SSL_KEY_PASSWORD_CONFIG"));

            XMLMessagesDispatcher dispatcher = new XMLMessagesDispatcher();
            dispatcher.setKafkaStreamsApplicationID(prop.getProperty("KAFKA_STREAMS_APPLICATION_ID"));
            dispatcher.setStreamThreadNumbers(prop.getProperty("NUM_STREAM_THREADS_CONFIG"));
            dispatcher.setBootstrapServerConfig(prop.getProperty("BOOTSTRAPSERVER_CONFIG"));
            dispatcher.setFromTopicName(prop.getProperty("FROM_TOPIC"));
            dispatcher.setToTopicName(prop.getProperty("TO_TOPIC"));
            dispatcher.setXmlMessageTagLocalName(prop.getProperty("XML_TAG_LOCALNAME"));
            dispatcher.setMatchExpression(prop.getProperty("MATCH_CONTENT"));

            dispatcher.setSecurityProtocolConfig(prop.getProperty("SECURITY_PROTOCOL_CONFIG"));
            dispatcher.setSaslMechanism(prop.getProperty("SASL_MECHANISM"));
            dispatcher.setSslKeypasswordConfig(prop.getProperty("SSL_KEY_PASSWORD_CONFIG"));
            dispatcher.setSslKeystoreLocation(prop.getProperty("SSL_KEYSTORE_LOCATION_CONFIG"));
            dispatcher.setSslKeystorePasswordConfig(prop.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG"));
            dispatcher.setSslTruststoreLocation(prop.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG"));
            dispatcher.setSslTruststorePassword(prop.getProperty("SSL_TRUSTSTORE_PASSWORD_CONFIG"));

            dispatcher.startProcess();

        } catch (Exception e) {
            LOGGER.info("java -Djava.security.auth.login.config=<path of jaas.conf> "
                    + "  -Dlog4j.configuration=file:<path of log4j.properties>"
                    + " -jar kafkastreamsapp-<VERSION>-jar-with-dependencies.jar <path of config.properties>");
            try {
                LOGGER.error("Error happened at KafkaStreamsApp ", e);
                input.close();
            } catch (IOException ex) {
                LOGGER.error("Error happened at KafkaStreamsApp ", ex);
            }
        }
    }
}
