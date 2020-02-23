package com.dwarszawski.airflowsink;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

public class AirflowSinkConnectorConfigTest {
    private ConfigDef configDef = AirflowSinkConnectorConfig.conf();

    @Test
    public void initialConfigIsValid() {
        assert (configDef.validate(TestConfig.initialConfig())
                .stream()
                .allMatch(configValue -> configValue.errorMessages().size() == 0));
    }
}