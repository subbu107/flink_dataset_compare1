package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.informatica.datavalidation.filter.MismatchFilter;
import com.informatica.datavalidation.pojo.Hashed;
import com.informatica.datavalidation.util.Utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.time.Time;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GenerateDetailedReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDetailedReport.class);

    private static List<String> leftFields = null;

    private static List<String> rightFields = null;

    private static List<String> leftKeyFields = null;

    private static List<String> rightKeyFields = null;
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        leftFields = Arrays.asList(new String[]{"id", "name", "val"});
        rightFields = Arrays.asList(new String[]{"id", "name", "val"});
        leftKeyFields = Arrays.asList(new String[]{"id"});
        rightKeyFields = Arrays.asList(new String[]{"id"});
    }

    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<JsonNode> kafkaTopic1 = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("localhost:9095")
                .setTopics("topic1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new GenericJsonDeserializationSchema()))
                .build();


        DataStreamSource<JsonNode> source1 = env.fromSource(kafkaTopic1, WatermarkStrategy.noWatermarks(), "source1");

        System.out.println("Received source1 sysout");

        List<Hashed> hashedList = getHashedData();

//        Set<String> keysHashSet = hashedList.stream().filter(h -> h.getLeftKeyHash().equals(h.getRightKeyHash()) && !h.getLeftValueHash().equals(h.getRightValueHash()))
//                .map(h -> h.getLeftKeyHash())
//                .collect(Collectors.toSet());

        /*SingleOutputStreamOperator<Tuple2<JsonNode, String>> hashedSrc1 = source1.filter(new MismatchFilter(keysHashSet, keyFields))
                .flatMap((x, y) -> y.collect(Tuple2.of(x, Utils.calculateKeyHash(keyFields, x)))); */

        SingleOutputStreamOperator<Tuple2<JsonNode, String>> hashedSrc1 = source1
                .flatMap(new FlatMapFunc1());

        KafkaSource<JsonNode> kafkaTopic2 = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("localhost:9095")
                .setTopics("topic2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new GenericJsonDeserializationSchema()))
                .build();

        DataStreamSource<JsonNode> source2 = env.fromSource(kafkaTopic2, WatermarkStrategy.noWatermarks(), "source2");

//        SingleOutputStreamOperator<Tuple2<JsonNode, String>> hashedSrc2 = source2.filter(new MismatchFilter(keysHashSet, keyFields))
//                .flatMap((x, y) -> y.collect(Tuple2.of(x, Utils.calculateKeyHash(keyFields, x))));

        SingleOutputStreamOperator<Tuple2<JsonNode, String>> hashedSrc2 = source2
                .flatMap(new FlatMapFunc1());

        DataStream<Tuple6<String, String, String, String, String, String>> mismatchData = hashedSrc1.join(hashedSrc2)
                .where(x -> x.f1)
                .equalTo(y -> y.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30).toDuration()))
                .apply(new JoinFunc1())
                .flatMap(new FlatMapFunc2());


        if (params.has("output")) {
            mismatchData.writeAsText(params.get("output"));
        } else {
            mismatchData.print();
        }

        JobExecutionResult result = env.execute("gen1");
    }

    public static final class FlatMapFunc1 implements FlatMapFunction<JsonNode, Tuple2<JsonNode, String>>, ResultTypeQueryable {

        @Override
        public void flatMap(JsonNode value, Collector<Tuple2<JsonNode, String>> out) {
            out.collect(Tuple2.of(value, Utils.calculateKeyHash(leftKeyFields, value)));
        }

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(new TypeHint<SingleOutputStreamOperator<Tuple2<JsonNode, String>>>() {
            });
        }
    }

    public static final class FlatMapFunc2 implements FlatMapFunction<Tuple2<JsonNode, JsonNode>, Tuple6<String, String, String , String , String, String>>, ResultTypeQueryable {
        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(new TypeHint<SingleOutputStreamOperator<Tuple6<String, String, String , String , String, String>>>() {
            });
        }

        public void calculateHash(List<String> leftFields, List<String> rightFields, JsonNode left, JsonNode right, Collector<Tuple6<String, String, String , String , String, String>> out) {
            String leftRowHash = Utils.calculateKeyHash(leftFields, left);
            String rightRowHash = Utils.calculateKeyHash(rightFields, right);

            if (!StringUtils.equals(leftRowHash, rightRowHash)) {
                if (leftFields.size() == 1) {

                    Tuple6<String, String, String , String , String, String> reportTuple = new Tuple6<>();

                    String leftMismatchField = leftFields.get(0);
                    String rightMismatchField = rightFields.get(0);

                    reportTuple.setField(StringUtils.join(leftKeyFields, "|"), 0);
                    reportTuple.setField(StringUtils.join(rightKeyFields, "|"), 1);
                    reportTuple.setField(leftMismatchField, 2);
                    reportTuple.setField(left.get(leftMismatchField), 3);
                    reportTuple.setField(rightMismatchField, 4);
                    reportTuple.setField(right.get(rightMismatchField), 5);


                    out.collect(reportTuple);
                } else {
                    calculateHash(leftFields.subList(0, leftFields.size() / 2), rightFields.subList(0,
                            rightFields.size() / 2), left, right, out);
                    calculateHash(leftFields.subList(leftFields.size() / 2, rightFields.size()),
                            rightFields.subList(rightFields.size() / 2, rightFields.size()), left, right, out);
                }
            }
        }

        @Override
        public void flatMap(Tuple2<JsonNode, JsonNode> value, Collector<Tuple6<String, String, String , String , String, String>> out) {
            calculateHash(leftFields, rightFields, value.f0, value.f1, out);
        }
    }

    public static final class JoinFunc1 implements JoinFunction<Tuple2<JsonNode, String>, Tuple2<JsonNode, String>,
            Tuple2<JsonNode, JsonNode>>, ResultTypeQueryable {

        @Override
        public Tuple2<JsonNode, JsonNode> join(Tuple2<JsonNode, String> first, Tuple2<JsonNode, String> second) throws Exception {
            return new Tuple2<>(first.f0, second.f0);
        }

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(new TypeHint<Tuple2<JsonNode, JsonNode>>() {
            });
        }
    }

    static List<Hashed> getHashedData() {

        List<Hashed> hashedList = new ArrayList<>();

        hashedList.add(new Hashed("1", "KH1", "VH1", "1", "KH1", "VH1"));
        hashedList.add(new Hashed("2", "KH2", "VH2", "2", "KH2", "VH2"));
        hashedList.add(new Hashed("3", "KH3", "VH3", null, null, null));
        hashedList.add(new Hashed(null, null, null, "4", "KH4", "VH4"));

        return hashedList;
    }
}