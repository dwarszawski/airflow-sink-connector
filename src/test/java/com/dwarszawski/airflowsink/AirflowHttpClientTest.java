package com.dwarszawski.airflowsink;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;

public class AirflowHttpClientTest {
    AirflowSinkConnectorConfig config = new AirflowSinkConnectorConfig(TestConfig.initialConfig());
    AirflowHttpClient client = new AirflowHttpClient(config);

    public static Schema KEY_SCHEMA = SchemaBuilder.struct().name("test")
            .version(1)
            .field("runId", Schema.STRING_SCHEMA)
            .build();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    @Test
    public void shouldProcessSinkRecords() throws InterruptedException {
        String expectedResponseBody = "{\"message\":\"Created <DagRun test_dag_run_id @ 2019-10-09 12:38:37+00:00: test, externally triggered: True>\"}";

        wireMockRule.stubFor(
                post(urlEqualTo("/api/experimental/dags/test_dag_id/dag_runs"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .withRequestBody(matchingJsonPath("$.run_id"))
                        .withRequestBody(matchingJsonPath("$.replace_microseconds"))
                        .withRequestBody(matchingJsonPath("$.conf"))
                        .willReturn(aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(expectedResponseBody))
        );


        String sampleRequestBody = "{\"field1\": [\"elem1\",\"elem2\"],\"field2\":false,\"field3\":\"20191016\"}";

        Collection<SinkRecord> records = Collections.singleton(
                new SinkRecord("stub",
                        0,
                        KEY_SCHEMA,
                        new Struct(KEY_SCHEMA)
                                .put("runId", "test_dag_run_id"),
                        null,
                        sampleRequestBody,
                        0));

        client.process(records);
        Thread.sleep(3000); // required for async http call to verify stub
    }
}
