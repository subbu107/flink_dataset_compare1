package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class GenericJsonDeserializationSchema implements KafkaDeserializationSchema<JsonNode> {


    private static final Logger LOGGER = LoggerFactory.getLogger(GenericJsonDeserializationSchema.class);


    private static final long serialVersionUID = -1L;

    private ObjectMapper mapper;


    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaDeserializationSchema.super.open(context);
    }

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        return false;
    }

    @Override
    public JsonNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        JsonNode actualObj = mapper.readTree(record.value());

        LOGGER.info("deserialized to "+actualObj);

        return actualObj;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<JsonNode> out) throws Exception {
        KafkaDeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return getForClass(JsonNode.class);
    }
}
