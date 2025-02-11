package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var inputStream = streamsBuilder.stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));


        inputStream.print(Printed.<String, String>toSysOut().withLabel("aggregate"));

        var groupedStream = inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        // var groupedStream = inputStream.groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()));
        //  exploreCount(groupedStream);

        exploreReduce(groupedStream);

        return streamsBuilder.build();
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {
        var reducedStream = groupedStream.reduce((value1, value2) -> {
            log.info("value1: {}, value2: {}", value1, value2);

            return value1.toUpperCase() + "-" + value2.toUpperCase();
        });
        reducedStream
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("reduced-words"));

    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {

        var countByAlphabet = groupedStream
                .count(Named.as("count-by-alphabet"));

        countByAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
    }

}
