package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DataFlowStream {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Integer> stream = builder.stream("streams-dataflow-input");
        stream.foreach((key, value) -> System.out.println("Key and value: " + key + ", " + value));
        stream.to("streams-dataflow-output");
        KStream<String, Integer> filteredStream = stream.filter((x, y) -> (y % 2) == 1)
                .map((k, v) -> new KeyValue<>(k, v * 3));
        Topology topo = builder.build();
        System.out.println(topo.describe());

        try (KafkaStreams streams = new KafkaStreams(topo, props)) {
            streams.start();
        }
    }
}
