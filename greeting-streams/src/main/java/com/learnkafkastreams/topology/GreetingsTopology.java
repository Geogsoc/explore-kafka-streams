package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingsTopology {

    public static String GREETINGS = "greetings";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static String GREETINGS_SPANISH = "greetings_spanish";


    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //  var mergedStream = getStringGreetingKStream(streamsBuilder);

        var mergedStream = getCustomGreetingKStream(streamsBuilder);

        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("greetingsStream"));

        var modifiedStream = mergedStream
//                .filterNot((readOnlyKey, value) -> value.length() > 4)
                .mapValues((readOnlyKey, value) ->

                        new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp()));
        //  .filter((key, value) -> key != null)
        //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//                    return newValues.stream().map(val -> KeyValue.pair(key, val.toUpperCase())).collect(Collectors.toList());
        //    });

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE
                , Produced.with(Serdes.String(), SerdesFactory.genericGreetingSerdes())
        );

        return streamsBuilder.build();
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS
                //              , Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                //             ,Consumed.with(Serdes.String(), Serdes.String())
        );

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);
        return mergedStream;
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, Greeting> greetingsStream = streamsBuilder.stream(GREETINGS
                , Consumed.with(Serdes.String(), SerdesFactory.genericGreetingSerdes())
        );

        KStream<String, Greeting> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                , Consumed.with(Serdes.String(), SerdesFactory.genericGreetingSerdes())
        );

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);
        return mergedStream;
    }


}
