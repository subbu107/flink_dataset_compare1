package com.informatica.datavalidation;

import javassist.Loader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.DocFlavor;
import java.util.ArrayList;
import java.util.List;

public class CsvCompare1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvCompare1.class);

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    private static final String EMPTY_FIELD_ACCUMULATOR = "empty-fields";

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //StreamExecutionEnvironment

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<SimplePojo> dataSet1 = getDataSet1(env, params);
        DataSet<SimplePojo> dataSet2 = getDataSet2(env, params);

        SortPartitionOperator<SimplePojo> sortedDataset1 = dataSet1.sortPartition("id", Order.ASCENDING);
        SortPartitionOperator<SimplePojo> sortedDataset2 = dataSet2.sortPartition("id", Order.ASCENDING);

        FlatMapOperator<SimplePojo, Tuple2<SimplePojo, String>> dataSet1Hash = sortedDataset1.flatMap(new HashComputer());
        FlatMapOperator<SimplePojo, Tuple2<SimplePojo, String>> dataSet2Hash = sortedDataset2.flatMap(new HashComputer());



        JoinOperator.DefaultJoin<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>> hashedJoin =
                dataSet1Hash.join(dataSet2Hash).
                where(x->((SimplePojo)x.getField(0)).getId()).
                        equalTo(y->((SimplePojo)y.getField(0)).getId());


        FilterOperator<Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>>> filteredMismatches = hashedJoin.filter(new RichFilterFunction<Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>>>() {
            @Override
            public boolean filter(Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>> value) throws Exception {
                Tuple2<SimplePojo, String> lhs = value.getField(0);
                Tuple2<SimplePojo, String> rhs = value.getField(1);
                return !lhs.getField(1).equals(rhs.getField(1));

            }
        });

        FlatMapOperator<Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>>, String> tuple2StringFlatMapOperator = filteredMismatches.flatMap(new FlatMapFunction<Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>>, String>() {
            @Override
            public void flatMap(Tuple2<Tuple2<SimplePojo, String>, Tuple2<SimplePojo, String>> value, Collector<String> out) throws Exception {
                Tuple2<SimplePojo, String> pojoTuple = value.getField(0);
                SimplePojo pojo = pojoTuple.getField(0);
                out.collect(pojo.getId());
            }
        });

        if (params.has("output")) {
            tuple2StringFlatMapOperator.writeAsText(params.get("output"));
        } else {
            tuple2StringFlatMapOperator.print();
        }

//
//                where(x -> (((SimplePojo)x.getField(0)).getId())..equalTo((SimplePojo)y.s -> y.getId());
//
//        simplePojoSimplePojoDefaultJoin.map()
//

        JobExecutionResult result = env.execute("compare 1");



        System.out.println("result ");
        // get the accumulator result via its registration key
        final List<Integer> emptyFields = result.getAccumulatorResult(EMPTY_FIELD_ACCUMULATOR);
        System.out.format("Number of detected empty fields per column: %s\n", emptyFields);    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************


    public static final class HashComputer implements FlatMapFunction<SimplePojo, Tuple2<SimplePojo, String>> {

        @Override
        public void flatMap(SimplePojo value, Collector<Tuple2<SimplePojo, String>> out) throws Exception {
            String pojoString = String.join("|", String.join(":", "Id", value.getId()),
                    String.join(":", "Name", value.getName()),
                    String.join(":", "Val", value.getVal()));
            String md5Hex = DigestUtils.md5Hex(pojoString);
            out.collect(Tuple2.of(value, md5Hex));
        }
    }




    @SuppressWarnings("unchecked")
    private static DataSet<SimplePojo> getDataSet1(
            ExecutionEnvironment env, MultipleParameterTool params) {
        if (params.has("input1")) {
            return env.readCsvFile(params.get("input1"))
                    .fieldDelimiter(",")
                    .lineDelimiter("\n")
                    //.types(String.class, String.class, String.class)
                    .pojoType(SimplePojo.class, "id", "name", "val")
                    ;
        } else {
            throw new RuntimeException("no input");
        }
    }


    @SuppressWarnings("unchecked")
    private static DataSet<SimplePojo> getDataSet2(
            ExecutionEnvironment env, MultipleParameterTool params) {
        if (params.has("input2")) {
            return env.readCsvFile(params.get("input2"))
                    .fieldDelimiter(",")
                    .lineDelimiter("\n")
                    //.types(String.class, String.class, String.class)
                    .pojoType(SimplePojo.class, "id", "name", "val")
                    ;
        } else {
            throw new RuntimeException("no input");
        }
    }


    public static class SimplePojo {

        private String id;
        private String name;
        private String val;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVal() {
            return val;
        }

        public void setVal(String val) {
            this.val = val;
        }
    }
}
