package de.micha.tweetservice.kafka;

import de.micha.tweetservice.serialize.KafkaSerializerDeserializer;
import de.micha.events.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Created by mherttrich on 16/05/18.
 */

@Component
public class KafkaStreamService {




    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KafkaStreamService.class);

    @Value("${kafka.topic.listening.name}")
    private String topic_listen;

    @Value("${kafka.topic.sending.name}")
    private String topic_sending;

    @Value("${kafka.brokers}")
    private String brokers;


    @Qualifier("kafkaProperties")
    @Autowired
    Properties properties;


    private static final Predicate<String, Tweet> filter = (k, tweet) -> tweet.getMessage().contains("important");

    @PostConstruct
    public void startStream() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-service");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        KafkaSerializerDeserializer<Tweet> serializer = new KafkaSerializerDeserializer<>(Tweet.class);
        KafkaSerializerDeserializer<Tweet> deserializer = new KafkaSerializerDeserializer<>(Tweet.class);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(topic_listen, Consumed.with(Serdes.String(), deserializer))
                .filter(filter)
                .map((k, v) -> KeyValue.pair(k, convert(v)))
                .to(Serdes.String(), serializer, topic_sending);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


    private Tweet convert(Tweet tweet) {
            return Tweet.newBuilder()
                    .setId(tweet.getId())
                    //TODO change in avro file
                    .setSize((long) tweet.getMessage().length())
                    .setMessage(tweet.getMessage().toLowerCase())
                    .setIp(evaluateIp())
                    .build();


    }

    private String evaluateIp() {
        //Todo
        return "127.0.0.1";
    }

}
