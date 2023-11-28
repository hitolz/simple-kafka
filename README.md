# simple-kafka

为了更方便的在 Rust 中使用 kafka。

## example
https://github.com/hitolz/simple-kafka-example


## 使用方法

读取配置文件，并将其转换为 simple_kafka::KafkaConfig
```toml
[kafka_config]
brokers = "localhost:9092"
group_id = "test_group2"
```

```rust
#[derive(Deserialize, Default, Debug,Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
}

impl Into<simple_kafka::KafkaConfig> for KafkaConfig {
    fn into(self) -> simple_kafka::KafkaConfig {
        simple_kafka::KafkaConfig{
            brokers: self.brokers,
            group_id: self.group_id,
        }
    }
}
```

在 main 中，通过 tokio::spawn 线程初始化 kafka 生产者及消费者。
```rust
let _init_task = tokio::spawn(async {
    let simple_kafka_config:simple_kafka::KafkaConfig = kafka_config.to_owned().into();
    simple_kafka::kafka_init::init_producers(&simple_kafka_config).await;
    simple_kafka::kafka_init::init_consumers(&simple_kafka_config,"test-topic", message_handler).await;
});
```

如果有多个 topic 需要进行消费，需要 init_consumers 多次。

发送消息
```rust
// let _= kafka_producer::send(topic,"key","测试下kafka消息1111".as_bytes()).await;
// let _= kafka_producer::send_result(topic,"key","测试下kafka消息1111".as_bytes()).await;
let _= kafka_producer::send_timeout(topic,"key","测试下kafka消息1111".as_bytes(),Duration::from_secs(3)).await;
```

提供了 test_api，

在程序启动之后，可以通过 http://127.0.0.1:8088/test/send 进行发送测试。
由于启动时也初始化了消费者，所以也能消费到这个消息。
在日志 app.log 中有体现。

```
2023-10-09 22:16:15 INFO [simple_kafka::kafka_producer:19] create kafka producer,brokers=localhost:9092
2023-10-09 22:16:15 INFO [simple_kafka::kafka_init:24] init producer done
2023-10-09 22:16:15 INFO [simple_kafka:61] register consumer: "test-topic"
2023-10-09 22:16:15 INFO [simple_kafka:84] creating consumer topic:test-topic 
2023-10-09 22:16:30 INFO [simple_kafka::kafka_producer:46] 发送kafka消息：partition:None, headers:None, key:"key", topic:test-topic, msg:测试下kafka消息1111
2023-10-09 22:16:31 INFO [simple_kafka:106] kafka consumer message. message = [Message { ptr: 0x1547068e8 }]
2023-10-09 22:16:31 INFO [simple_kafka:163] partition = 0, offset = 1618 message : "测试下kafka消息1111"
```

