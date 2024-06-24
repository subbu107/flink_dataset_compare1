package com.informatica.datavalidation.filter;

import com.fasterxml.jackson.databind.JsonNode;

import com.informatica.datavalidation.util.Utils;
import org.apache.flink.api.common.functions.RichFilterFunction;

import java.util.List;
import java.util.Set;


public class MismatchFilter extends RichFilterFunction<JsonNode> {

    private Set<String> keysHashSet;

    private List<String> keyFields;
    public MismatchFilter(Set<String> keysHashSet, List<String> keyFields) {
        this.keysHashSet = keysHashSet;
        this.keyFields = keyFields;
    }
    @Override
    public boolean filter(JsonNode value) {
            return keysHashSet.contains(Utils.calculateKeyHash(keyFields, value));
    }
}