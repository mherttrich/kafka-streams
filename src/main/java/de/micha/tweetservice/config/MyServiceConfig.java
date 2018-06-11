package de.micha.tweetservice.config;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class MyServiceConfig {


    @Value("${kafka.brokers}")
    private String brokers;


    @Value("${service.name}")
    private String service_name;


    @Bean(name="kafkaProperties")

    public Properties configureKafkaProperties() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, service_name);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return config;
    }
}
