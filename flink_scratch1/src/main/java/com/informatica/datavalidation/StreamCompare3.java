package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StreamCompare3 {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCompare3.class);


    private static  List<String> fields = null;
    private static List<String> keyFields = null;

    private static String lhsTopic = "topic1";
    private static String rhsTopic = "topic2";
    private static Long testCaseId = 1L;
    private static Long testCaseRunId = 1L;

    static {
        fields = Arrays.asList(new String[]{"id", "name", "val"});
        keyFields = Arrays.asList(new String[]{"id"});
    }

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        setParams(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //JSONKeyValueDeserializationSchema

        KafkaSource<JsonNode> kafkaTopic1 = getKafkaSource(lhsTopic);
        KafkaSource<JsonNode> kafkaTopic2 = getKafkaSource(rhsTopic);

        DataStreamSource<JsonNode> streamSource1 = env.fromSource(kafkaTopic1, WatermarkStrategy.noWatermarks(), "lhsSource");
        DataStreamSource<JsonNode> streamSource2 = env.fromSource(kafkaTopic2, WatermarkStrategy.noWatermarks(), "rhsSource");


        SingleOutputStreamOperator<Hashed> stream1Hash = streamSource1.flatMap(new HashComputer(testCaseId, testCaseRunId, ComparisonSide.LEFT,fields, keyFields)).setMaxParallelism(8);
        SingleOutputStreamOperator<Hashed> stream2Hash = streamSource2.flatMap(new HashComputer(testCaseId, testCaseRunId, ComparisonSide.RIGHT,fields, keyFields)).setMaxParallelism(8);


        DataStream<Hashed> unionHash = stream1Hash.union(stream2Hash);
        SingleOutputStreamOperator<Hashed> storedMap = unionHash.map(new CacheStoreMap(fields, keyFields));




        KafkaRecordSerializationSchema hashedSerializationSchema = KafkaRecordSerializationSchema.builder().
                setTopic("topic-out1")
                .setValueSerializationSchema(new JsonSerializationSchema<Hashed>())
                .build();



        KafkaSink<Hashed> sink = KafkaSink.<Hashed>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(hashedSerializationSchema
                )

                .build();

        storedMap.sinkTo(sink);


        JobExecutionResult result = env.execute("hash1");

    }

    private static void setParams(MultipleParameterTool params) {
        if (params.has("testCaseId")) {
            testCaseId = params.getLong("testCaseId");
        }
        if (params.has("testCaseRunId")) {
            testCaseRunId = params.getLong("testCaseRunId");
        }
        if (params.has("keyFields")) {
            keyFields = Arrays.asList(params.get("keyFields").split(","));
        }

        if (params.has("fields")) {
            fields = Arrays.asList(params.get("fields").split(","));
        }

        if(params.has("lhsTopic")) {
            lhsTopic = params.get("lhsTopic");
        }

        if(params.has("rhsTopic")) {
            rhsTopic = params.get("rhsTopic");
        }
    }

    private static KafkaSource<JsonNode> getKafkaSource(String topic) {
        return KafkaSource.<JsonNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new GenericJsonDeserializationSchema()))
                .build();
    }


}
