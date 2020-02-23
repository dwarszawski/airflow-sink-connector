package com.dwarszawski.airflowsink;

import com.dwarszawski.airflowsink.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AirflowSinkConnector extends SourceConnector {
    private AirflowSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        config = new AirflowSinkConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AirflowSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(config.originalsStrings());
        return configs;

    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return AirflowSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.class.getPackage().getImplementationVersion();
    }
}
