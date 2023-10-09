use serde_derive::Deserialize;
use lazy_static::lazy_static;
use std::fs::File;
use std::env;
use std::io::prelude::*;


#[derive(Deserialize, Default, Debug,Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
}

#[derive(Deserialize, Default, Debug)]
pub struct Setting {
    pub kafka_config: KafkaConfig,

}

/// 获取toml相关配置
macro_rules! get_setting_from_toml {
    ($struct: ident) => {{
        let result = $struct::default();

        // 获取项目的目录路径
        // let current_dir = if let Ok(v) = env::current_dir() { v } else { return result; };
        // let current_path = if let Some(v) = current_dir.to_str() { v } else { return result; };
        let current_path = env!("CARGO_MANIFEST_DIR");
        // 读取配置文件
        let toml_file = format!("{}/configs/config.toml", current_path);
        match File::open(&toml_file) {
            Ok(mut v) => {
                let mut content = String::new();
                if let Ok(_) = v.read_to_string(&mut content) {
                    if let Ok(t) = toml::from_str::<$struct>(&content) {
                        t
                    } else {
                        result
                    }
                } else {
                    result
                }
            }
            Err(err) => {
                println!("读取文件失败: {}", err);
                result
            }
        }
    }};
}

lazy_static! {
    pub static ref SETTING: Setting = get_setting_from_toml!(Setting);
}



#[cfg(test)]
mod config_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_setting() {
        let setting = &*SETTING;
        println!("{:?}", setting);
    }

}