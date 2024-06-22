package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class StreamCompare1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCompare1.class);


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

        KafkaSource<ObjectNode> kafkaTopic1 = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topic1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(false)))
                .build();

        DataStreamSource<ObjectNode> source11 = env.fromSource(kafkaTopic1, WatermarkStrategy.noWatermarks(), "source1");

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


    public static final class HashComputer implements FlatMapFunction<ObjectNode, Hashed> {

        @Override
        public void flatMap(ObjectNode value, Collector<Hashed> out) throws Exception {

            LOGGER.info("got value : "+value);
            StringBuilder valueBuilder = new StringBuilder();
            StringBuilder keyBuilder = new StringBuilder();

            for(String fieldName : fields){
                LOGGER.info("Getting fieldName: "+fieldName);
                JsonNode fieldValueNode = value.get(fieldName);
                if(fieldValueNode == null){
                    valueBuilder.append("").append("|");
                } else {
                    valueBuilder.append(fieldValueNode.textValue()!=null ? fieldValueNode.textValue() : "" ).append("|");
                }
            }
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode root = mapper.createObjectNode();

            for(String fieldName : fields) {
                LOGGER.info("Getting fieldName: "+fieldName);
                JsonNode fieldValueNode = value.get(fieldName);
                if(fieldValueNode == null) {
                    keyBuilder.append(fieldName+"|"+"").append("|");;
                } else {
                    keyBuilder.append(fieldName+"|"+fieldValueNode.textValue()).append("|");
                }

                //root.put(fieldName, fieldName);
            }

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
