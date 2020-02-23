package com.dwarszawski.airflowsink;

import java.util.HashMap;
import java.util.Map;

import static com.dwarszawski.airflowsink.AirflowSinkConnectorConfig.*;

public class TestConfig {

    public static Map<String, String> initialConfig() {
        Map<String, String> baseProps = new HashMap<>();
        baseProps.put(AIRFLOW_ENDPOINT_CONFIG, "http://127.0.0.1:8089");
        baseProps.put(AIRFLOW_USER_CONFIG, "user");
        baseProps.put(AIRFLOW_PASSWORD_CONFIG, "pass");
        baseProps.put(AIRFLOW_DAG_ID, "test_dag_id");
        baseProps.put(AIRFLOW_DAG_RUN_ID, "$.runId");

        return baseProps;
    }
}
