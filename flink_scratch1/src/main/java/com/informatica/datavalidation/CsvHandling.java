package com.informatica.datavalidation;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CsvHandling {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvHandling.class);

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    private static final String EMPTY_FIELD_ACCUMULATOR = "empty-fields";

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<SimplePojo> dataSet = getDataSet(env, params);

        // filter lines with empty fields
        final DataSet<SimplePojo> filteredLines = dataSet.filter(new EmptyFieldFilter());

        long count = filteredLines.count();
        System.out.println("count of filtered lines : "+count);
        // Here, we could do further processing with the filtered lines...
        JobExecutionResult result;
        // output the filtered lines
        if (params.has("output")) {
            //filteredLines.writeAsCsv(params.get("output"));
            //filteredLines.writeAsCsv(params.get("output"), "\n", ",");

            filteredLines.writeAsText(params.get("output"));
            // execute program
            result = env.execute("Accumulator example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            filteredLines.print();
            result = env.getLastJobExecutionResult();
        }

        System.out.println("result ");
        // get the accumulator result via its registration key
        final List<Integer> emptyFields = result.getAccumulatorResult(EMPTY_FIELD_ACCUMULATOR);
        System.out.format("Number of detected empty fields per column: %s\n", emptyFields);    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * This function filters all incoming tuples that have one or more empty fields. In doing so, it
     * also counts the number of empty fields per attribute with an accumulator (registered under
     *
     */
    public static final class EmptyFieldFilter extends RichFilterFunction<SimplePojo> {

        // create a new accumulator in each filter function instance
        // accumulators can be merged later on
        private final VectorAccumulator emptyFieldCounter = new VectorAccumulator();

        @Override
        public void open(final OpenContext openContext) throws Exception {
            super.open(openContext);

            // register the accumulator instance
            getRuntimeContext().addAccumulator(EMPTY_FIELD_ACCUMULATOR, this.emptyFieldCounter);
        }

        // iterate over the tuple fields looking for empty ones

        @Override
        public boolean filter(final SimplePojo t) {
            boolean containsEmptyFields = false;

            if(StringUtils.isBlank(t.getId())) {
                this.emptyFieldCounter.add(0);
                containsEmptyFields = true;
            }
            if(StringUtils.isBlank(t.getName())) {
                this.emptyFieldCounter.add(1);
                containsEmptyFields = true;
            }
            if(StringUtils.isBlank(t.getVal())) {
                this.emptyFieldCounter.add(2);
                containsEmptyFields = true;
            }

            return !containsEmptyFields;
        }
    }


    @SuppressWarnings("unchecked")
    private static DataSet<SimplePojo> getDataSet(
            ExecutionEnvironment env, MultipleParameterTool params) {
        if (params.has("input")) {
            return env.readCsvFile(params.get("input"))
                    .fieldDelimiter(",")
                    .lineDelimiter("\n")
                    //.types(String.class, String.class, String.class)
                    .pojoType(SimplePojo.class, "id", "name", "val")
            ;
        } else {
            throw new RuntimeException("no input");
        }


    }


    /**
     * This accumulator maintains a vector of counts. Calling {@link #add(Integer)} increments the
     * <i>n</i>-th vector component. The size of the vector is automatically managed.
     */
    public static class VectorAccumulator implements Accumulator<Integer, ArrayList<Integer>> {

        /** Stores the accumulated vector components. */
        private final ArrayList<Integer> resultVector;

        public VectorAccumulator() {
            this(new ArrayList<Integer>());
        }

        public VectorAccumulator(ArrayList<Integer> resultVector) {
            this.resultVector = resultVector;
        }

        /** Increases the result vector component at the specified position by 1. */
        @Override
        public void add(Integer position) {
            updateResultVector(position, 1);
        }

        /**
         * Increases the result vector component at the specified position by the specified delta.
         */
        private void updateResultVector(int position, int delta) {
            // inflate the vector to contain the given position
            while (this.resultVector.size() <= position) {
                this.resultVector.add(0);
            }

            // increment the component value
            final int component = this.resultVector.get(position);
            this.resultVector.set(position, component + delta);
        }

        @Override
        public ArrayList<Integer> getLocalValue() {
            return this.resultVector;
        }

        @Override
        public void resetLocal() {
            // clear the result vector if the accumulator instance shall be reused
            this.resultVector.clear();
        }

        @Override
        public void merge(final Accumulator<Integer, ArrayList<Integer>> other) {
            // merge two vector accumulators by adding their up their vector components
            final List<Integer> otherVector = other.getLocalValue();
            for (int index = 0; index < otherVector.size(); index++) {
                updateResultVector(index, otherVector.get(index));
            }
        }

        @Override
        public Accumulator<Integer, ArrayList<Integer>> clone() {
            return new VectorAccumulator(new ArrayList<Integer>(resultVector));
        }

        @Override
        public String toString() {
            return StringUtils.join(resultVector, ',');
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
