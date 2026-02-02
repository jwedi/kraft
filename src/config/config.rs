use std::fmt;
use std::ops::Add;
use std::path::Path;
use std::sync::Arc;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub persistence_dir: String,
    pub port: u32,
    pub metrics_port: u32,
    pub node_id: u32,
    pub cluster_nodes: Vec<ClusterNode>,
    pub max_message_size_bytes: usize,
    pub min_batch_interval_ms: u64,
    pub max_batch_size: u64,
    pub enable_otel_tracing: bool,
    #[serde(default)]
    pub use_capnp_transport: bool,
    #[serde(default)]
    pub capnp_port: u32,
    #[serde(default)]
    pub tcp_datastore_port: u32,
}


#[derive(Debug, Deserialize, Clone)]
pub struct ClusterNode {
    pub endpoint: String,
    pub node_id: u32,
    /// Cap'n Proto TCP endpoint (e.g., "[::1]:50151")
    #[serde(default)]
    pub capnp_endpoint: String,
}

pub fn read_config() -> Result<AppConfig, Box<dyn std::error::Error>> {
    // Initialize a Config builder
    let resource_dir = std::env::var("RESOURCE_DIR").unwrap_or_else(|_| "resources".to_string());
    let default_search_locations = vec![
        resource_dir.clone(),  // Environment variable-based folder
        "src/resources".to_string(),
        ".".to_string(),           // Current directory
    ];
    let override_search_locations = default_search_locations.clone();

    let mut builder = Config::builder();
    for location in default_search_locations {
        let file = "/application.toml";
        let file_name = location.add(file);
        let fn_string = file_name.as_str();
        if Path::new(fn_string).exists() {
            builder = builder.add_source(File::with_name(fn_string).required(false));
        }
    }

    for location in override_search_locations {
        let file = "/local.toml";
        let file_name = location.add(file);
        let fn_string = file_name.as_str();
        if Path::new(fn_string).exists() {
            builder = builder.add_source(File::with_name(fn_string).required(false));
        }
    }
    builder = builder.add_source(Environment::with_prefix("APP"));
    let conf = builder.build()?;

    // Deserialize into a strongly-typed struct
    let app_config: AppConfig = conf.try_deserialize()?;

    println!("{:#?}", app_config);

    Ok(app_config)
}
