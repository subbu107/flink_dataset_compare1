package com.informatica.datavalidation.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;

public class Utils {

    public static  String calculateKeyHash(List<String> keyFields, JsonNode value) {

        StringBuilder keyBuilder = new StringBuilder();
        for(String fieldName : keyFields) {
            JsonNode fieldValueNode = value.get(fieldName);
            keyBuilder.append(fieldValueNode).append("|");
        }
        return DigestUtils.md5Hex(keyBuilder.toString());
    }
}
