package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Greeting> GreetingSerdes() {
        return new GreetingSerdes();

    }

    public static Serde<Greeting> genericGreetingSerdes() {
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    }


}
