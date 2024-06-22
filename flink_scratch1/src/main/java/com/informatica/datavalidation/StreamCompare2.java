package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StreamCompare2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCompare2.class);


    private static  List<String> fields = null;
    private static List<String> keyFields = null;

    static {
        fields = Arrays.asList(new String[]{"id", "name", "val"});
        keyFields = Arrays.asList(new String[]{"id"});
    }

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //JSONKeyValueDeserializationSchema

        KafkaSource<JsonNode> kafkaTopic1 = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topic1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new GenericJsonDeserializationSchema()))
                .build();

        DataStreamSource<JsonNode> source11 = env.fromSource(kafkaTopic1, WatermarkStrategy.noWatermarks(), "source1");

        SingleOutputStreamOperator<Hashed> hashedSingleOutputStreamOperator = source11.flatMap(new HashComputer());

        //KafkaRecordSerializationSchema.builder().



        KafkaRecordSerializationSchema hashedSerializationSchema = KafkaRecordSerializationSchema.builder().
                setTopic("topic-out1")
                .setValueSerializationSchema(new JsonSerializationSchema<Hashed>())
                .build();



        KafkaSink<Hashed> sink = KafkaSink.<Hashed>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(hashedSerializationSchema
                )

                .build();

        hashedSingleOutputStreamOperator.sinkTo(sink);

        JobExecutionResult result = env.execute("hash1");



    }


    public static final class HashComputer implements FlatMapFunction<JsonNode, Hashed> {

        @Override
        public void flatMap(JsonNode value, Collector<Hashed> out) throws Exception {

            LOGGER.info("got value : "+value);
            StringBuilder valueBuilder = new StringBuilder();
            StringBuilder keyBuilder = new StringBuilder();


            LOGGER.info("------------building value str -----------");
            for(String fieldName : fields){
                LOGGER.info("Getting fieldName: "+fieldName);
                JsonNode fieldValueNode = value.get(fieldName);
                LOGGER.info("Field value : "+fieldValueNode);
                valueBuilder.append( fieldValueNode  ).append("|");

            }

            LOGGER.info("------------building key str -----------");
            for(String fieldName : keyFields) {
                LOGGER.info("Getting fieldName: "+fieldName);
                JsonNode fieldValueNode = value.get(fieldName);
                LOGGER.info("field value "+fieldValueNode);
                keyBuilder.append(fieldValueNode).append("|");
            }

            LOGGER.info("value list "+valueBuilder.toString());
            LOGGER.info("key list "+keyBuilder.toString());
            String valueHash = DigestUtils.md5Hex(valueBuilder.toString());
            String keyHash = DigestUtils.md5Hex(keyBuilder.toString());

            out.collect(new Hashed(keyBuilder.toString(), keyHash, valueHash));
        }
    }

    private static class Hashed {
        String keys;
        private String keyHash;
        private String valueHash;

        public Hashed(String keys, String keyHash, String valueHash) {
            this.keys = keys;
            this.keyHash = keyHash;
            this.valueHash = valueHash;
        }

        public String getKeys() {
            return keys;
        }

        public void setKeys(String keys) {
            this.keys = keys;
        }

        public String getKeyHash() {
            return keyHash;
        }

        public void setKeyHash(String keyHash) {
            this.keyHash = keyHash;
        }

        public String getValueHash() {
            return valueHash;
        }

        public void setValueHash(String valueHash) {
            this.valueHash = valueHash;
        }
    }
}
