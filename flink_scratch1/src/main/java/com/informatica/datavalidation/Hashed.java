package com.informatica.datavalidation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class Hashed {
    Long testCaseId;
    Long testCaseRunId;
    ComparisonSide comparisonSide;
    String keys;
    private String keyHash;
    private String valueHash;


}
