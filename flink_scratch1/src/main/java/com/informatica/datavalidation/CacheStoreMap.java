package com.informatica.datavalidation;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public final class CacheStoreMap extends RichMapFunction<Hashed, Hashed> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheStoreMap.class);
    String storedProc = "call cdv_db.check_and_delete(?, ?, ?,?, ?,?)";
    private List<String> fields;
    private List<String> keyFields;
    private Connection con;


    public CacheStoreMap(List<String> fields, List<String> keyFields) {
        this.fields = fields;
        this.keyFields = keyFields;
    }


    @Override
    public Hashed map(Hashed value) throws Exception {

        try (CallableStatement stmt = con.prepareCall(storedProc)) {
            stmt.setLong(1, value.getTestCaseId());
            stmt.setLong(2, value.getTestCaseRunId());
            stmt.setString(3, value.getKeys());
            stmt.setString(4, value.getKeyHash());
            stmt.setString(5, value.getValueHash());
            stmt.setBoolean(6, ComparisonSide.LEFT.equals(value.getComparisonSide()));
            stmt.execute();
        }
        return value;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(
                "jdbc:mysql://invcdvswagger02:3306/cdv_db", "cdv_user", "Infa@123");


    }
}
