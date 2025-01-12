package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class GreetingsDeserializer implements Deserializer<Greeting> {

    private ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greeting deserialize(String s, byte[] data) {

        try {
            return objectMapper.readValue(data, Greeting.class);
        } catch (IOException e) {
            log.error("JsonProcessingException : {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception : {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
