package com.informatica.datavalidation;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@AllArgsConstructor
public final class HashComputer implements FlatMapFunction<JsonNode, Hashed> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashComputer.class);

    private Long testCaseId;
    private Long testCaseRunId;

    private ComparisonSide comparisonSide;

    private List<String> fields;
    private List<String> keyFields;



    @Override
    public void flatMap(JsonNode value, Collector<Hashed> out) throws Exception {

        LOGGER.info("got value : " + value);
        StringBuilder valueBuilder = new StringBuilder();
        StringBuilder keyBuilder = new StringBuilder();


        LOGGER.info("------------building value str -----------");
        for (String fieldName : fields) {
            LOGGER.info("Getting fieldName: " + fieldName);
            JsonNode fieldValueNode = value.get(fieldName);
            LOGGER.info("Field value : " + fieldValueNode);
            valueBuilder.append(fieldValueNode).append("|");

        }

        LOGGER.info("------------building key str -----------");
        for (String fieldName : keyFields) {
            LOGGER.info("Getting fieldName: " + fieldName);
            JsonNode fieldValueNode = value.get(fieldName);
            LOGGER.info("field value " + fieldValueNode);
            keyBuilder.append(fieldValueNode).append("|");
        }

        LOGGER.info("value list " + valueBuilder.toString());
        LOGGER.info("key list " + keyBuilder.toString());
        String valueHash = DigestUtils.md5Hex(valueBuilder.toString());
        String keyHash = DigestUtils.md5Hex(keyBuilder.toString());

        out.collect(new Hashed(testCaseId, testCaseRunId, comparisonSide, keyBuilder.toString(), keyHash, valueHash));
    }

}
