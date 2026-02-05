use config::{Config, Environment, File};
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::ops::Add;
use std::path::Path;

/// Configuration validation error
#[derive(Debug)]
pub struct ConfigValidationError {
    pub field: String,
    pub message: String,
}

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Config validation error in '{}': {}", self.field, self.message)
    }
}

impl std::error::Error for ConfigValidationError {}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub persistence_dir: String,
    pub metrics_port: u32,
    pub node_id: u32,
    pub cluster_nodes: Vec<ClusterNode>,
    pub max_message_size_bytes: usize,
    pub min_batch_interval_ms: u64,
    pub max_batch_size: u64,
    pub enable_otel_tracing: bool,
    /// Port for inter-cluster communication
    pub cluster_port: u32,
    /// Port for external RPC access
    #[serde(default)]
    pub tcp_datastore_port: u32,
}

impl AppConfig {
    /// Validates the configuration and returns a list of errors if any.
    pub fn validate(&self) -> Result<(), Vec<ConfigValidationError>> {
        let mut errors = Vec::new();

        // Validate max_message_size_bytes range
        const MIN_MESSAGE_SIZE: usize = 1024; // 1 KB minimum
        const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB maximum
        if self.max_message_size_bytes < MIN_MESSAGE_SIZE {
            errors.push(ConfigValidationError {
                field: "max_message_size_bytes".to_string(),
                message: format!(
                    "Must be at least {} bytes, got {}",
                    MIN_MESSAGE_SIZE, self.max_message_size_bytes
                ),
            });
        }
        if self.max_message_size_bytes > MAX_MESSAGE_SIZE {
            errors.push(ConfigValidationError {
                field: "max_message_size_bytes".to_string(),
                message: format!(
                    "Must be at most {} bytes, got {}",
                    MAX_MESSAGE_SIZE, self.max_message_size_bytes
                ),
            });
        }

        // Validate max_batch_size range
        const MAX_BATCH_SIZE_LIMIT: u64 = 10_000;
        if self.max_batch_size == 0 {
            errors.push(ConfigValidationError {
                field: "max_batch_size".to_string(),
                message: "Must be greater than 0".to_string(),
            });
        }
        if self.max_batch_size > MAX_BATCH_SIZE_LIMIT {
            errors.push(ConfigValidationError {
                field: "max_batch_size".to_string(),
                message: format!("Must be at most {}, got {}", MAX_BATCH_SIZE_LIMIT, self.max_batch_size),
            });
        }

        // Validate min_batch_interval_ms range
        const MAX_BATCH_INTERVAL: u64 = 60_000; // 1 minute max
        if self.min_batch_interval_ms > MAX_BATCH_INTERVAL {
            errors.push(ConfigValidationError {
                field: "min_batch_interval_ms".to_string(),
                message: format!(
                    "Must be at most {}ms, got {}",
                    MAX_BATCH_INTERVAL, self.min_batch_interval_ms
                ),
            });
        }

        // Validate port ranges
        if self.metrics_port > 65535 {
            errors.push(ConfigValidationError {
                field: "metrics_port".to_string(),
                message: format!("Invalid port number: {}", self.metrics_port),
            });
        }
        if self.cluster_port > 65535 {
            errors.push(ConfigValidationError {
                field: "cluster_port".to_string(),
                message: format!("Invalid port number: {}", self.cluster_port),
            });
        }
        if self.tcp_datastore_port > 65535 {
            errors.push(ConfigValidationError {
                field: "tcp_datastore_port".to_string(),
                message: format!("Invalid port number: {}", self.tcp_datastore_port),
            });
        }

        // Validate cluster topology
        self.validate_cluster_topology(&mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn validate_cluster_topology(&self, errors: &mut Vec<ConfigValidationError>) {
        // Check for duplicate node IDs
        let mut seen_ids = HashSet::new();
        for node in &self.cluster_nodes {
            if !seen_ids.insert(node.node_id) {
                errors.push(ConfigValidationError {
                    field: "cluster_nodes".to_string(),
                    message: format!("Duplicate node_id: {}", node.node_id),
                });
            }
        }

        // Check for self-reference (node referencing itself in cluster_nodes)
        // This is allowed but logged as a warning since the node will ignore itself
        for node in &self.cluster_nodes {
            if node.node_id == self.node_id {
                log::warn!(
                    "Node {} includes itself in cluster_nodes; this entry will be ignored",
                    self.node_id
                );
            }
        }

        // Validate endpoint format
        for node in &self.cluster_nodes {
            if node.endpoint.is_empty() {
                errors.push(ConfigValidationError {
                    field: "cluster_nodes".to_string(),
                    message: format!("Node {} has empty endpoint", node.node_id),
                });
            } else if node.endpoint.parse::<std::net::SocketAddr>().is_err() {
                errors.push(ConfigValidationError {
                    field: "cluster_nodes".to_string(),
                    message: format!(
                        "Node {} has invalid endpoint '{}': expected format 'host:port'",
                        node.node_id, node.endpoint
                    ),
                });
            }
        }

        // Check minimum cluster size for consensus (need at least 3 nodes for majority)
        let total_nodes = self.cluster_nodes.len() + 1; // +1 for self
        if total_nodes < 3 {
            // This is a warning, not an error - single-node clusters are valid for development
            log::warn!(
                "Cluster has only {} node(s). At least 3 nodes are recommended for fault tolerance.",
                total_nodes
            );
        }
    }

    /// Returns a safe representation of the config for logging (hides sensitive data if any)
    pub fn safe_display(&self) -> String {
        format!(
            "AppConfig {{ node_id: {}, cluster_port: {}, metrics_port: {}, \
             tcp_datastore_port: {}, cluster_nodes: {} nodes, \
             max_message_size_bytes: {}, max_batch_size: {}, enable_otel_tracing: {} }}",
            self.node_id,
            self.cluster_port,
            self.metrics_port,
            self.tcp_datastore_port,
            self.cluster_nodes.len(),
            self.max_message_size_bytes,
            self.max_batch_size,
            self.enable_otel_tracing
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterNode {
    pub endpoint: String,
    pub node_id: u32,
}

pub fn read_config() -> Result<AppConfig, Box<dyn std::error::Error>> {
    // Initialize a Config builder
    let resource_dir = std::env::var("RESOURCE_DIR").unwrap_or_else(|_| "resources".to_string());
    let default_search_locations = vec![
        resource_dir.clone(), // Environment variable-based folder
        "src/resources".to_string(),
        ".".to_string(), // Current directory
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

    // Validate the configuration
    if let Err(validation_errors) = app_config.validate() {
        let error_messages: Vec<String> = validation_errors.iter().map(|e| e.to_string()).collect();
        return Err(format!(
            "Configuration validation failed:\n  - {}",
            error_messages.join("\n  - ")
        )
        .into());
    }

    // Use safe display instead of Debug to avoid logging sensitive data
    println!("Configuration loaded: {}", app_config.safe_display());

    Ok(app_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_valid_config() -> AppConfig {
        AppConfig {
            persistence_dir: "/tmp/test".to_string(),
            metrics_port: 9090,
            node_id: 1,
            cluster_nodes: vec![
                ClusterNode {
                    endpoint: "127.0.0.1:50051".to_string(),
                    node_id: 2,
                },
                ClusterNode {
                    endpoint: "127.0.0.1:50052".to_string(),
                    node_id: 3,
                },
            ],
            max_message_size_bytes: 1024 * 1024, // 1MB
            min_batch_interval_ms: 100,
            max_batch_size: 1000,
            enable_otel_tracing: false,
            cluster_port: 50050,
            tcp_datastore_port: 8080,
        }
    }

    // =========================================================================
    // Regression test: Config validation catches invalid message size
    // Fix: Added validation for max_message_size_bytes range
    // =========================================================================
    #[test]
    fn test_validate_message_size_too_small() {
        let mut config = create_valid_config();
        config.max_message_size_bytes = 100; // Below MIN_MESSAGE_SIZE (1024)

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "max_message_size_bytes"));
    }

    #[test]
    fn test_validate_message_size_too_large() {
        let mut config = create_valid_config();
        config.max_message_size_bytes = 100 * 1024 * 1024; // Above MAX_MESSAGE_SIZE (64MB)

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "max_message_size_bytes"));
    }

    // =========================================================================
    // Regression test: Config validation catches invalid batch size
    // Fix: Added validation for max_batch_size range
    // =========================================================================
    #[test]
    fn test_validate_batch_size_zero() {
        let mut config = create_valid_config();
        config.max_batch_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "max_batch_size"));
    }

    #[test]
    fn test_validate_batch_size_too_large() {
        let mut config = create_valid_config();
        config.max_batch_size = 20_000; // Above MAX_BATCH_SIZE_LIMIT (10_000)

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "max_batch_size"));
    }

    // =========================================================================
    // Regression test: Config validation catches invalid port numbers
    // Fix: Added validation for port ranges
    // =========================================================================
    #[test]
    fn test_validate_invalid_port() {
        let mut config = create_valid_config();
        config.metrics_port = 70000; // Invalid port (> 65535)

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "metrics_port"));
    }

    // =========================================================================
    // Regression test: Config validation catches duplicate node IDs
    // Fix: Added cluster topology validation
    // =========================================================================
    #[test]
    fn test_validate_duplicate_node_ids() {
        let mut config = create_valid_config();
        config.cluster_nodes = vec![
            ClusterNode {
                endpoint: "127.0.0.1:50051".to_string(),
                node_id: 2,
            },
            ClusterNode {
                endpoint: "127.0.0.1:50052".to_string(),
                node_id: 2,
            }, // Duplicate!
        ];

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.field == "cluster_nodes" && e.message.contains("Duplicate")));
    }

    // =========================================================================
    // Regression test: Config validation catches invalid endpoint format
    // Fix: Added endpoint format validation
    // =========================================================================
    #[test]
    fn test_validate_invalid_endpoint_format() {
        let mut config = create_valid_config();
        config.cluster_nodes = vec![ClusterNode {
            endpoint: "not-a-valid-endpoint".to_string(),
            node_id: 2,
        }];

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.field == "cluster_nodes" && e.message.contains("invalid endpoint")));
    }

    #[test]
    fn test_validate_empty_endpoint() {
        let mut config = create_valid_config();
        config.cluster_nodes = vec![ClusterNode {
            endpoint: "".to_string(),
            node_id: 2,
        }];

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.field == "cluster_nodes" && e.message.contains("empty endpoint")));
    }

    // =========================================================================
    // Regression test: Valid config passes validation
    // =========================================================================
    #[test]
    fn test_valid_config_passes() {
        let config = create_valid_config();
        let result = config.validate();
        assert!(result.is_ok());
    }

    // =========================================================================
    // Regression test: Safe display doesn't expose sensitive data
    // Fix: Added safe_display method for logging
    // =========================================================================
    #[test]
    fn test_safe_display_format() {
        let config = create_valid_config();
        let display = config.safe_display();

        // Should contain key info
        assert!(display.contains("node_id: 1"));
        assert!(display.contains("cluster_port: 50050"));
        assert!(display.contains("2 nodes")); // Cluster size, not full details

        // Should NOT contain full cluster node details
        assert!(!display.contains("127.0.0.1:50051"));
    }
}
