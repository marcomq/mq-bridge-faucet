//! The same implementation, reached two ways: through the factory linked into
//! this test binary, and through the factory the host builds from the compiled
//! plugin library. Identical results mean the ABI round trip kept the
//! endpoint's behaviour, and that the Python and npm packages get the same one.

use mq_bridge::errors::{ConsumerError, InvalidConfig};
use mq_bridge::plugin::load_endpoint_plugin;
use mq_bridge::plugin::test_support::build_plugin_cdylib;
use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge::SentBatch;
use mq_bridge_faucet::FaucetFactory;
use serde_json::json;

/// Copies a CSV file to a JSONL file through both endpoints and returns the output.
async fn copied(factory: &dyn CustomEndpointFactory) -> String {
    let dir = tempfile::tempdir().expect("a temp dir");
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n").expect("write the csv");

    let mut consumer = factory
        .create_consumer(
            "route",
            &json!({"connector": "csv", "config": {"path": input}}),
        )
        .await
        .expect("create the consumer");
    let publisher = factory
        .create_publisher(
            "route",
            &json!({"connector": "jsonl", "config": {"path": output}}),
        )
        .await
        .expect("create the publisher");

    loop {
        let batch = match consumer.receive_batch(2).await {
            Ok(batch) => batch,
            Err(ConsumerError::EndOfStream) => break,
            Err(error) => panic!("the source failed: {error}"),
        };
        let acks = vec![MessageDisposition::Ack; batch.messages.len()];
        let sent = publisher
            .send_batch(batch.messages)
            .await
            .expect("send the batch");
        assert!(matches!(sent, SentBatch::Ack));
        (batch.commit)(acks).await.expect("commit the batch");
    }
    publisher.flush().await.expect("flush the publisher");
    std::fs::read_to_string(&output).expect("read the jsonl")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_endpoint_behaves_the_same_linked_directly_and_loaded_as_a_plugin() {
    let direct = copied(&FaucetFactory).await;
    assert_eq!(direct.lines().count(), 3);
    assert!(direct.contains("alice") && direct.contains("carol"));

    let library = build_plugin_cdylib(".", "mq-bridge-faucet").expect("build the plugin");
    let info = load_endpoint_plugin(&library).expect("load the plugin");
    assert_eq!(info.name, "faucet");
    let factory = mq_bridge::extensions::get_endpoint_factory(&info.name)
        .expect("loading a plugin registers its endpoint");

    assert_eq!(copied(factory.as_ref()).await, direct);
}

#[tokio::test]
async fn a_rejected_configuration_stops_the_route_instead_of_reconnecting() {
    for value in [
        json!({"connector": "no-such-connector"}),
        json!({"connector": "csv", "config": {"path": "in.csv"}, "extra": true}),
        json!({"connector": "csv", "config": {"no_such_field": 1}}),
    ] {
        let consumer = FaucetFactory.create_consumer("route", &value).await;
        assert!(
            consumer.err().is_some_and(|e| e.is::<InvalidConfig>()),
            "{value}"
        );
    }
    let publisher = FaucetFactory
        .create_publisher("route", &json!({"connector": "no-such-connector"}))
        .await;
    assert!(publisher.err().is_some_and(|e| e.is::<InvalidConfig>()));
}

#[test]
fn the_schema_requires_a_connector() {
    let schema = FaucetFactory.config_schema().expect("a schema");
    assert_eq!(schema["required"], json!(["connector"]));
    assert_eq!(schema["x-mqb-default-batch-size"], json!(1000));
}
