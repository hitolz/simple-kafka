use lazy_static::lazy_static;
use log::info;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::KafkaError;

lazy_static! {
    static ref PRODUCER: Arc<Mutex<Option<FutureProducer>>> = Arc::new(Mutex::new(None));
}

/// kakfa生产者
fn create_producer(brokers: &str) -> FutureProducer {
    info!("create kafka producer,brokers={}", brokers);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "1")
        .create()
        .expect("Failed to create producer");
    producer
}

pub fn init(brokers: &str) {
    let mut guard = PRODUCER.lock().unwrap();
    if guard.is_none() {
        let producer = create_producer(brokers);
        *guard = Some(producer);
    }
}

/// rename to send_result
#[deprecated]
pub async fn send(
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let producer = get_producer();
    let message = FutureRecord::to(topic).key(key).payload(payload);
    let msg = String::from_utf8(payload.to_vec())?;
    info!(
        "发送kafka消息：partition:{:?}, headers:{:?}, key:{:?}, topic:{}, msg:{}",
        message.partition, message.headers, key, topic, msg
    );
    match producer.send_result(message) {
        Ok(delivery_future) => match delivery_future.await {
            Ok(_) => Ok(()),
            Err(_err) => Err(Box::new(KafkaError::new(format!(
                "Kafka send failed : topic={},key={},msg={}",
                key, topic, msg
            ))))
            .unwrap(),
        },
        Err((err, _)) => Err(Box::new(err.to_string())).unwrap(),
    }
}

/// See also the FutureProducer::send_result method, which will not retry the queue operation if the queue is full.
pub async fn send_result(
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let producer = get_producer();
    let message = FutureRecord::to(topic).key(key).payload(payload);
    let msg = String::from_utf8(payload.to_vec())?;
    info!(
        "发送kafka消息：partition:{:?}, headers:{:?}, key:{:?}, topic:{}, msg:{}",
        message.partition, message.headers, key, topic, msg
    );
    match producer.send_result(message) {
        Ok(delivery_future) => match delivery_future.await {
            Ok(_) => Ok(()),
            Err(_err) => Err(Box::new(KafkaError::new(format!(
                "Kafka send failed : topic={},key={},msg={}",
                key, topic, msg
            ))))
            .unwrap(),
        },
        Err((err, _)) => Err(Box::new(err.to_string())).unwrap(),
    }
}

fn get_producer() -> FutureProducer {
    let gard = PRODUCER.lock().unwrap();
    if gard.is_none() {
        let e = KafkaError::new("Kafka producer not initialized".to_string());
        return Err(Box::new(e)).unwrap();
    }
    let producer = gard.as_ref().unwrap();
    producer.clone()
}

///  FutureProducer::send with timeout
pub async fn send_timeout(
    topic: &str,
    key: &str,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let producer = get_producer();
    let message = FutureRecord::to(topic).key(key).payload(payload);
    let msg = String::from_utf8(payload.to_vec())?;
    info!(
        "发送kafka消息：partition:{:?}, headers:{:?}, key:{:?}, topic:{}, msg:{}",
        message.partition, message.headers, key, topic, msg
    );
    match producer.send(message, timeout).await {
        Ok(_) => Ok(()),
        Err((err, _)) => Err(Box::new(err.to_string())).unwrap(),
    }
}
