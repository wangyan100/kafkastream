/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.yw.kafka;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * @author yanwang this message dispatcher reads message which value is xml
 * string and based on expression to pick up matched message and dispatch it to
 * destination topic
 */
public class XMLMessagesDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(XMLMessagesDispatcher.class);

    private String kafkaStreamsApplicationID;
    private String xmlMessageTagLocalName;
    private String matchExpression;
    private String toTopicName;
    private String fromTopicName;
    private String bootstrapServerConfig;
    private String streamThreadNumbers;
    private String securityProtocolConfig;
    private String saslMechanism;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreLocation;
    private String sslKeystorePasswordConfig;
    private String sslKeypasswordConfig;

    public String getStreamThreadNumbers() {
        return streamThreadNumbers;
    }

    public void setStreamThreadNumbers(String streamThreadNumbers) {
        this.streamThreadNumbers = streamThreadNumbers;
    }

    public String getXmlMessageTagLocalName() {
        return xmlMessageTagLocalName;
    }

    public void setXmlMessageTagLocalName(String xmlMessageTagLocalName) {
        this.xmlMessageTagLocalName = xmlMessageTagLocalName;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSecurityProtocolConfig() {
        return securityProtocolConfig;
    }

    public void setSecurityProtocolConfig(String securityProtocolConfig) {
        this.securityProtocolConfig = securityProtocolConfig;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePasswordConfig() {
        return sslKeystorePasswordConfig;
    }

    public void setSslKeystorePasswordConfig(String sslKeystorePasswordConfig) {
        this.sslKeystorePasswordConfig = sslKeystorePasswordConfig;
    }

    public String getSslKeypasswordConfig() {
        return sslKeypasswordConfig;
    }

    public void setSslKeypasswordConfig(String sslKeypasswordConfig) {
        this.sslKeypasswordConfig = sslKeypasswordConfig;
    }

    public String getKafkaStreamsApplicationID() {
        return kafkaStreamsApplicationID;
    }

    public void setKafkaStreamsApplicationID(String kafkaStreamsApplicationID) {
        this.kafkaStreamsApplicationID = kafkaStreamsApplicationID;
    }

    public String getMatchExpression() {
        return matchExpression;
    }

    public void setMatchExpression(String matchExpression) {
        this.matchExpression = matchExpression;
    }

    public String getToTopicName() {
        return toTopicName;
    }

    public void setToTopicName(String toTopicName) {
        this.toTopicName = toTopicName;
    }

    public String getFromTopicName() {
        return fromTopicName;
    }

    public void setFromTopicName(String fromTopicName) {
        this.fromTopicName = fromTopicName;
    }

    public String getBootstrapServerConfig() {
        return bootstrapServerConfig;
    }

    public void setBootstrapServerConfig(String bootstrapServerConfig) {
        this.bootstrapServerConfig = bootstrapServerConfig;
    }

    private boolean checkConfigs() {

        if (kafkaStreamsApplicationID == null || xmlMessageTagLocalName == null || matchExpression == null || toTopicName == null || fromTopicName == null || bootstrapServerConfig == null) {
            return false;
        } else {
            return true;
        }
    }

    public boolean checkMatchs(String messageValue, String tagLocalName, String matchpattern) {
        boolean matched = false;
        try {
            String textContentFromNode = XMLParser.getTextContentFromTagLocalName(messageValue, "*", tagLocalName);
            matched = XMLParser.checkMatchs(textContentFromNode, matchpattern);
            return matched;
        } catch (Exception e) {
            logger.error("Exception occurs  ", e);
            return matched;
        }
    }

    public void startProcess() {
        if (checkConfigs()) {
            logger.info("XMLMessagesDispatcher  startProcess is running ......");
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    this.getKafkaStreamsApplicationID());
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    this.getBootstrapServerConfig());
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,this.getStreamThreadNumbers());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass().getName());
            props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, this.getSecurityProtocolConfig());
            props.put(SaslConfigs.SASL_MECHANISM, this.getSaslMechanism());
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.getSslTruststoreLocation());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.getSslTruststorePassword());

            /*
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.getSslKeystoreLocation());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.getSslKeystorePasswordConfig());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getSslKeypasswordConfig());
             */
            // serialize and deserialize format for message
            final Serde<String> stringSerde = Serdes.String();
            Consumed<String, String> types = Consumed.with(stringSerde, stringSerde);
            //create StreamFactory
            StreamsBuilder builder = new StreamsBuilder();
            //read message from topic
            KStream<String, String> xmlMessages = builder.stream(this.getFromTopicName(), types);

            //select matched messages
            KStream<String, String> matchedMessages = xmlMessages.filter((key, xmlMessageValue) -> {
                logger.debug("xml message key: {} value: {}", key, xmlMessageValue);
                return checkMatchs(xmlMessageValue, this.getXmlMessageTagLocalName(), this.getMatchExpression());
            });

            //dispatch matched message to destination topic
            matchedMessages.to(this.getToTopicName());

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        } else {
            logger.info("one of properties are not set up, the process will not be started ......");
        }
    }
}
