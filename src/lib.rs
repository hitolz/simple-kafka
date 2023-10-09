/// kafka 初始化生产者和消费者
pub mod kafka_init;
/// 提供一个生产者，发送 kafka 消息使用
pub mod kafka_producer;
/// 配置信息，配置文件在根目录下的 configs/config.toml
pub mod settings;

use std::{error::Error, fmt::Display};

use std::{collections::HashMap, sync::Arc};
use settings::KafkaConfig;
use time::{macros::format_description, UtcOffset};
use tracing::{info, warn};

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::OwnedMessage,
    ClientConfig, Message,
};


struct KafkaConsumer {
    topic: String,
    func: Box<dyn (FnMut(KafkaMessage)) + 'static + Send>,
}

impl KafkaConsumer {
    pub fn new<F>(topic: &str, func: F) -> Self
    where
        F: 'static + Send,
        F: FnMut(KafkaMessage),
    {
        KafkaConsumer {
            topic: topic.to_owned(),
            func: Box::new(func),
        }
    }
}

struct KafkaConsumerManager {
    brokers: String,
    group_id: String,
    consumers: Vec<KafkaConsumer>,
}

impl KafkaConsumerManager {
    pub fn new(brokers: &str, group_id: &str) -> Self {
        KafkaConsumerManager {
            brokers: brokers.to_owned(),
            group_id: group_id.to_owned(),
            consumers: Vec::new(),
        }
    }

    pub fn register_consumer<F>(&mut self, topic: &str, func: F)
    where
        F: 'static + Send,
        F: FnMut(KafkaMessage),
    {
        info!("register consumer: {:?}", topic);
        let consumer = KafkaConsumer::new(topic, func);
        self.consumers.push(consumer);
    }

    pub async fn start(self) {
        let shared_data: Arc<KafkaConfig> = Arc::new(KafkaConfig {
            brokers: self.brokers,
            group_id: self.group_id,
        });

        for consumer in self.consumers {
            KafkaConsumerManager::create_consumer(shared_data.clone(), consumer).await;
        }
    }

    async fn create_consumer(shared_data: Arc<KafkaConfig>, mut consumer: KafkaConsumer) {
        let topic: String = consumer.topic.clone();
        let topics: Vec<&str> = vec![topic.as_str()];

        let brokers: String = shared_data.brokers.clone();
        let group_id: String = shared_data.group_id.clone();

        info!("creating consumer topic:{} ", topic,);

        let stream_consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        // 订阅主题
        stream_consumer
            .subscribe(&topics.to_vec())
            .expect("Can't subscribe to specified topics");

        tokio::spawn(async move {
            loop {
                match stream_consumer.recv().await {
                    Err(e) => warn!("kafka error: {}", e),
                    Ok(m) => {
                        info!("kafka consumer message. message = [{:#?}]", m);
                        let message: KafkaMessage = KafkaMessage::from(m.detach());
                        (consumer.func)(message);
                    }
                }
            }
        });
    }
}

#[derive(Default, Debug)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
    pub headers: Option<HashMap<String, String>>,
}

impl From<OwnedMessage> for KafkaMessage {
    fn from(v: OwnedMessage) -> Self {
        KafkaMessage {
            topic: v.topic().to_owned(),
            partition: v.partition(),
            offset: v.offset(),
            key: v.key().map(|v| v.to_vec()),
            value: v.payload().map(|v| v.to_vec()),
            timestamp: v.timestamp().to_millis(),
            headers: Some(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct KafkaError {
    message: String,
}
impl Error for KafkaError {}

impl Display for KafkaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl KafkaError {
    pub fn new(message: String) -> Self {
        KafkaError { message }
    }
}
/// consumer 消费消息的测试方法
pub fn message_handler(message: KafkaMessage) {
    let partition = message.partition;
    if let Some(value) = &message.value {
        let value = String::from_utf8_lossy(value);
        info!(
            "partition = {:#?}, offset = {:?} message : {:#?}",
            partition, message.offset, value
        );
    }
}
#[allow(dead_code)]
fn init_log() {
    use tracing_subscriber::fmt::time::OffsetTime;
    let local_time = OffsetTime::new(
        UtcOffset::from_hms(8, 0, 0).unwrap(),
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"),
    );

    tracing_subscriber::fmt().with_timer(local_time).init();
}

#[cfg(test)]
mod tests {

    // use chrono::Local;

    // use crate::common::date_utils::DateUtil;
    // use crate::plugins::log as DinosaurLog;

    use crate::{
        init_log,
        settings::SETTING,
        {kafka_init, kafka_producer, message_handler, KafkaConsumerManager},
    };
    use std::thread;
    use std::time::Duration;

    // use super::kafka::KafkaConfig;

    #[tokio::test]
    async fn test_produce() {
        // DinosaurLog::log_init();
        init_log();
        let topic: &str = "test-topic";
        kafka_init::init_producers().await;
        println!("send");
        for i in 0..100 {
            // let dt = DateUtil::<Local>::local().get_now_time_str();
            let message = format!("test : {}", i);
            let _ = kafka_producer::send(topic, "", message.as_bytes()).await;
        }
    }

    #[tokio::test]
    async fn test_consume() {
        init_log();
        let topic: &str = "test-topic";
        kafka_init::init_consumers(topic, message_handler).await;

        thread::sleep(Duration::from_secs(10));
    }
}
