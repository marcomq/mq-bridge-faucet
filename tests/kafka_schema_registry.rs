//! Kafka with Confluent Avro through the plugin: records written by the faucet
//! Kafka sink, encoded against a schema registry, read back by the faucet Kafka
//! source. Needs a broker and a registry, e.g. Redpanda, at
//! `FAUCET_TEST_KAFKA_BROKERS` and `FAUCET_TEST_SCHEMA_REGISTRY`.

use mq_bridge::errors::ConsumerError;
use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge::{CanonicalMessage, SentBatch};
use mq_bridge_faucet::FaucetFactory;
use serde_json::{Value, json};

const SCHEMA: &str = r#"{"type":"record","name":"Person","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this test"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Kafka and a schema registry"]
async fn avro_records_round_trip_through_the_schema_registry() {
    let brokers = env("FAUCET_TEST_KAFKA_BROKERS");
    let registry = json!({"url": env("FAUCET_TEST_SCHEMA_REGISTRY")});
    let topic = format!("faucet-avro-{}", std::process::id());
    let records = vec![
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "name": "bob"}),
        json!({"id": 3, "name": "carol"}),
    ];

    let publisher = FaucetFactory
        .create_publisher(
            "route",
            &json!({"connector": "kafka", "flush": "batch", "config": {
                "brokers": brokers,
                "topic": {"type": "fixed", "name": topic},
                "value_format": {"type": "confluent_avro", "schema_registry": registry},
                "value_schema": SCHEMA,
            }}),
        )
        .await
        .expect("create the publisher");
    let messages = records
        .iter()
        .map(|record| CanonicalMessage::from_json(record.clone()).expect("a message"))
        .collect();
    let sent = publisher
        .send_batch(messages)
        .await
        .expect("send the batch");
    assert!(matches!(sent, SentBatch::Ack));
    publisher.flush().await.expect("flush the publisher");

    let mut consumer = FaucetFactory
        .create_consumer(
            "route",
            &json!({"connector": "kafka", "config": {
                "brokers": brokers,
                "topics": [topic],
                "group_id": format!("{topic}-reader"),
                "auto_offset_reset": "earliest",
                "max_messages": records.len(),
                "idle_timeout": 30,
                "value_format": {"type": "confluent_avro", "schema_registry": registry},
            }}),
        )
        .await
        .expect("create the consumer");
    let mut read = Vec::new();
    loop {
        let batch = match consumer.receive_batch(10).await {
            Ok(batch) => batch,
            Err(ConsumerError::EndOfStream) => break,
            Err(error) => panic!("the source failed: {error}"),
        };
        for message in &batch.messages {
            read.push(serde_json::from_slice::<Value>(&message.payload).expect("a JSON record"));
        }
        let acks = vec![MessageDisposition::Ack; batch.messages.len()];
        (batch.commit)(acks).await.expect("commit the batch");
    }

    let mut values: Vec<Value> = read
        .into_iter()
        .map(|record| record["value"].clone())
        .collect();
    values.sort_by_key(|value| value["id"].as_i64());
    assert_eq!(values, records);
}
