package com.example.kafkastreamsgroupbywindowed;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.WindowedSerdes.timeWindowedSerdeFrom;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.giladam.kafka.jacksonserde.Jackson2Serde;
import com.giladam.kafka.jacksonserde.Jackson2Serializer;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.With;

@SpringBootApplication
public class KafkaStreamsGroupbyWindowedApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsGroupbyWindowedApplication.class);

    public static Properties buildProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tatapouet2");
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return properties;
    }

    public static Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = "input";
        String outputTopic = "output";

        Duration size = Duration.ofMillis(5 * 1000);

        ObjectMapper objectMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .findAndRegisterModules();

        Consumed<String, String> consumed = Consumed
                .with(Serdes.String(), Serdes.String());

        Serde<AggregatedCount> serdeAggregatedCount = new Jackson2Serde<>(objectMapper, AggregatedCount.class);

        builder
                .stream(inputTopic, consumed)
                .mapValues((k,v) -> {
                    try {
                        return Long.parseLong(v);
                    }
                    catch(Exception e) {
                        return null;
                    }
                })
                .filter((k,v) -> v != null)
                .groupByKey()
                .windowedBy(TimeWindows.of(size).grace(size))
                .aggregate(
                        () -> new AggregatedCount(),
                        (key, value, aggreg) -> aggreg.withNb(aggreg.getNb() + 1).withSum(aggreg.getSum() + value),
                        Materialized.<String, AggregatedCount, WindowStore<Bytes, byte[]>>as("count2")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdeAggregatedCount)
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v))
                .to(outputTopic, Produced.with(Serdes.String(), serdeAggregatedCount));

        return builder.build();
    }

    public static void main(String[] args) {
        Topology topology = buildTopology();

        logger.debug(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, buildProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.cleanUp();
        streams.start();
    }

}
