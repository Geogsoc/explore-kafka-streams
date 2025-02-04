package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Order> orderSerdes() {
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    }
    public static Serde<Revenue> revenueSerdes() {
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    }

}
