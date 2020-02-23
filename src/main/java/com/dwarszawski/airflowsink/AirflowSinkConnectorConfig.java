package com.dwarszawski.airflowsink;

import com.dwarszawski.airflowsink.validators.EndpointValidator;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AirflowSinkConnectorConfig extends AbstractConfig {

    static final String AIRFLOW_ENDPOINT_CONFIG = "airflow.endpoint";
    static final String AIRFLOW_USER_CONFIG = "airflow.username";
    static final String AIRFLOW_PASSWORD_CONFIG = "airflow.password";
    static final String AIRFLOW_DAG_ID = "airflow.dag.id";
    static final String AIRFLOW_DAG_RUN_ID = "airflow.dag.run.id";

    public AirflowSinkConnectorConfig(ConfigDef definition, Map<String, String> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public AirflowSinkConnectorConfig(Map<String, String> params) {
        this(conf(), params, false);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(AIRFLOW_ENDPOINT_CONFIG, Type.STRING, "http://127.0.0.1:8089", new EndpointValidator(), ConfigDef.Importance.HIGH, "airflow url")
                .define(AIRFLOW_USER_CONFIG, Type.STRING, ConfigDef.Importance.HIGH, "airflow user to authenticate http requests")
                .define(AIRFLOW_PASSWORD_CONFIG, Type.PASSWORD, ConfigDef.Importance.HIGH, "airflow password to authenticate http requests")
                .define(AIRFLOW_DAG_ID, Type.STRING, ConfigDef.Importance.HIGH, "airflow <DAG_ID> to invoke")
                .define(AIRFLOW_DAG_RUN_ID, Type.STRING, ConfigDef.Importance.HIGH, "json path to extract from kafka event key which represents <DAG_RUN_ID>");
    }

    public String getDagRunId() {
        return this.getString(AIRFLOW_DAG_RUN_ID);
    }

    public String getAirflowEndpoint() {
        //TODO enable dag_id templates parametrized with value from kafka event
        return String.format("%s/api/experimental/dags/%s/dag_runs", this.getString(AIRFLOW_ENDPOINT_CONFIG), this.getString(AIRFLOW_DAG_ID));
    }

    public String getAirflowUsername() {
        return this.getString(AIRFLOW_USER_CONFIG);
    }

    public String getAirflowPassword() {
        return this.getPassword(AIRFLOW_PASSWORD_CONFIG).value();
    }

}
