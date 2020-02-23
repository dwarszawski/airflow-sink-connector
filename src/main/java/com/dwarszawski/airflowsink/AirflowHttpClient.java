package com.dwarszawski.airflowsink;

import com.dwarszawski.airflowsink.utils.DataConverter;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class AirflowHttpClient {

    private static final Logger log = LoggerFactory.getLogger(AirflowHttpClient.class);

    AirflowSinkConnectorConfig config;

    public AirflowHttpClient(AirflowSinkConnectorConfig config) {
        this.config = config;
    }

    public void process(Collection<SinkRecord> records) {

        for (SinkRecord sinkRecord : records) {

            String jsonKeyRecord = DataConverter.convertKey(sinkRecord);
            String jsonValueRecord = DataConverter.convertValue(sinkRecord);

            String runId = extractFromKey(sinkRecord, config.getDagRunId());
            String requestBody = "{\"run_id\":\"" + runId + "\",\"replace_microseconds\":\"False\",\"conf\":" + jsonValueRecord + "}";

            Unirest
                    .post(config.getAirflowEndpoint())
                    .basicAuth(config.getAirflowUsername(), config.getAirflowPassword())
                    .header("Content-Type", "application/json")
                    .body(requestBody)
                    .asStringAsync(new Callback<String>() {
                        @Override
                        public void completed(HttpResponse<String> response) {
                            log.info(String.format("request [%s] completed with status [%s] and response [%s]", requestBody, response.getStatusText(), response.getBody()));
                        }

                        @Override
                        public void failed(UnirestException e) {
                            log.error(String.format("request [%s] failed with exception [%s])", requestBody, e.getMessage()));
                        }

                        @Override
                        public void cancelled() {
                            log.warn(String.format("request [%s] was cancelled", requestBody));
                        }
                    });
        }
    }

    private String extractFromKey(SinkRecord sinkRecord, String jsonPath) {
        String jsonKeyRecord = DataConverter.convertKey(sinkRecord);

        DocumentContext jsonContext = JsonPath.parse(jsonKeyRecord);
        return jsonContext.read(jsonPath);
    }
}
