use tracing::info;

use crate::{
    settings::{KafkaConfig, SETTING},
    KafkaMessage,
};

use super::{kafka_producer, KafkaConsumerManager};

//kafka初始化
// pub async fn init() {
//     let kafka_config: KafkaConfig = get_kafka_config();
//     init_producers(&kafka_config).await;
//     info!("init producer done");

//     // init_consumers(brokers, group_id).await;
//     // info!("init consumer done");
// }

/// 初始化生产者
pub async fn init_producers() {
    let kafka_config: KafkaConfig = get_kafka_config();
    kafka_producer::init(&kafka_config.brokers);
    info!("init producer done");
}

fn get_kafka_config() -> KafkaConfig {
    let setting = &*SETTING;
    setting.kafka_config.clone()
}

/// 初始化消费者
pub async fn init_consumers<F>(topic: &str, func: F)
where
    F: 'static + Send,
    F: FnMut(KafkaMessage),
{
    let kafka_config: KafkaConfig = get_kafka_config();
    let brokers = &kafka_config.brokers;
    let group_id = &kafka_config.group_id;
    let mut manager = KafkaConsumerManager::new(brokers.as_str(), group_id.as_str());
    manager.register_consumer(topic, func);
    let _ = tokio::spawn(async move {
        manager.start().await;
    })
    .await;
}
