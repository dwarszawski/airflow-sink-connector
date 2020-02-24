# Airflow Sink Connector

*   the Airflow sink connector allows you to export data from Kafka topics and trigger your Airflow DAG through REST API

## Prerequisites

*   `airflow-sink-connector-<VERSION>-jar-with-dependencies.jar` must be provided to your Kafka Connect cluster

## Quickstart

*   Airflow sink reads events from given topic, extract dag_run_id from <MESSAGE_KEY> given by the json path defined as `airflow.dag.run.id`
    and generate http call according to connector configuration.

    url formatting:
    ```
       <airflow.endpoint>/api/experimental/dags/<airflow.dag.id>/dag_runs
    ```
    payload formatting:
    ```
       {
         run_id: extract json path as defined in <airflow.dag.run.id> from <MESSAGE_KEY>,
         replace_microseconds: False,
         conf: extract json as passed with <MESSAGE_VALUE>
       }
    ```

*   Following configuration of kafka-connector should be used

    ```
       connector.class=com.dwarszawski.airflowsink.AirflowSinkConnector
       airflow.endpoint=<HOST>:<PORT>
       airflow.username=<USER_NAME>
       airflow.password=<YOUR_PASSWORD>
       airflow.dag.id=<DAG_ID>
       airflow.dag.run.id=<JSON_PATH_TO_EXTRACT_FROM_KAFKA_EVENT_KEY>
       topics=<TOPIC1>,<TOPIC2>
       tasks.max=1
       key.converter=org.apache.kafka.connect.json.JsonConverter
       value.converter=org.apache.kafka.connect.json.JsonConverter
       key.converter.schemas.enable=false,
       value.converter.schemas.enable=false,
    ```

*   Connector can be registered through HTTP API as follow

    ```
       source /env/variables;
       status_code="$(curl -s -o /dev/null -w "%{http_code}" -X POST -u "$KAFKA_CONNECT_USER:$KAFKA_CONNECT_PASSWORD" -H "Content-Type: application/json" --data '{
                 "name": "'$CONNECTOR_NAME'",
                 "config": {
                 "connector.class": "com.dwarszawski.airflowsink.airflowsink.AirflowSinkConnector",
                 "airflow.endpoint": "'$AIRFLOW_ENDPOINT'",
                 "airflow.username": "'$AIRFLOW_API_USERNAME'",
                 "airflow.password": "'$AIRFLOW_API_PASSWORD'",
                 "airflow.dag.id": "'$AIRFLOW_DAG_ID'",
                 "airflow.dag.run.id": "'$AIRFLOW_DAG_RUN_ID'",
                 "topics": "'$AIRFLOW_TOPICS'",
                 "tasks.max": "1",
                 "key.converter": "org.apache.kafka.connect.json.JsonConverter"
                 "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                 "key.converter.schemas.enable": "false",
                 "value.converter.schemas.enable": "false",
               }
               }' $KAFKA_CONNECT_HOST:$KAFKA_CONNECT_PORT/connectors)";
       echo "$status_code";
       if [[ "$status_code" == "201" ]] || [[ "$status_code" == "409" ]]; then exit 0; else  exit 1;  fi ;
    ```


