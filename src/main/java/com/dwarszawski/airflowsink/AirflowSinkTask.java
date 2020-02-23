package com.dwarszawski.airflowsink;

import com.dwarszawski.airflowsink.utils.Version;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class AirflowSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AirflowSinkTask.class);
    private AirflowHttpClient httpClient;
    private AirflowSinkConnectorConfig config;


    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new AirflowSinkConnectorConfig(props);
        httpClient = new AirflowHttpClient(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            httpClient.process(records);
        } catch (Exception e) {
            log.error("Error while processing records", e);
        }
    }

    @Override
    public void stop() {
    }
}


