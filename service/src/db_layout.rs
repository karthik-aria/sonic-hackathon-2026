use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const MONITORED_DATABASES: [&str; 3] = ["CONFIG_DB", "APPL_DB", "ASIC_DB"];

#[derive(Clone, Debug)]
pub(crate) struct Endpoint {
    pub(crate) socket: Option<PathBuf>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) password_file: Option<PathBuf>,
}

impl Endpoint {
    pub(crate) fn identity(&self) -> String {
        self.socket.as_ref().map_or_else(
            || format!("{}:{}", self.host, self.port),
            |socket| socket.display().to_string(),
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Database {
    pub(crate) id: u32,
    pub(crate) endpoint: Endpoint,
}

#[derive(Clone, Debug)]
pub(crate) struct MonitorSource {
    pub(crate) endpoint: Endpoint,
    pub(crate) databases: BTreeMap<u32, String>,
}

#[derive(Debug)]
pub(crate) struct Layout {
    databases: HashMap<String, Database>,
}

#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "INSTANCES")]
    instances: HashMap<String, RawInstance>,
    #[serde(rename = "DATABASES")]
    databases: HashMap<String, RawDatabase>,
}

#[derive(Deserialize)]
struct RawInstance {
    #[serde(default)]
    hostname: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default)]
    unix_socket_path: String,
    #[serde(default)]
    password_path: String,
}

#[derive(Deserialize)]
struct RawDatabase {
    id: DatabaseId,
    instance: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DatabaseId {
    Number(u32),
    Text(String),
}

impl DatabaseId {
    fn parse(self, name: &str) -> Result<u32> {
        match self {
            Self::Number(id) => Ok(id),
            Self::Text(id) => id
                .parse()
                .with_context(|| format!("invalid database id for {name}")),
        }
    }
}

const fn default_port() -> u16 {
    6379
}

impl Layout {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let content = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let raw: RawConfig = serde_json::from_slice(&content)
            .with_context(|| format!("parsing {}", path.display()))?;
        let mut databases = HashMap::with_capacity(raw.databases.len());
        for (name, raw_database) in raw.databases {
            let instance = raw
                .instances
                .get(&raw_database.instance)
                .with_context(|| format!("database {name} references missing Redis instance"))?;
            let id = raw_database.id.parse(&name)?;
            let endpoint = Endpoint {
                socket: (!instance.unix_socket_path.is_empty())
                    .then(|| PathBuf::from(&instance.unix_socket_path)),
                host: if instance.hostname.is_empty() {
                    "127.0.0.1".to_owned()
                } else {
                    instance.hostname.clone()
                },
                port: instance.port,
                password_file: (!instance.password_path.is_empty())
                    .then(|| PathBuf::from(&instance.password_path)),
            };
            drop(databases.insert(name.clone(), Database { id, endpoint }));
        }
        Ok(Self { databases })
    }

    pub(crate) fn database(&self, name: &str) -> Option<&Database> {
        self.databases.get(name)
    }

    pub(crate) fn monitor_sources(&self) -> Result<Vec<MonitorSource>> {
        let mut by_instance: BTreeMap<String, MonitorSource> = BTreeMap::new();
        for name in MONITORED_DATABASES {
            let database = self
                .databases
                .get(name)
                .with_context(|| format!("{name} is missing from database_config.json"))?;
            let identity = database.endpoint.identity();
            let source = by_instance
                .entry(identity)
                .or_insert_with(|| MonitorSource {
                    endpoint: database.endpoint.clone(),
                    databases: BTreeMap::new(),
                });
            if let Some(existing) = source.databases.get(&database.id)
                && existing != name
            {
                bail!("{name} and {existing} share a Redis database index");
            }
            drop(source.databases.insert(database.id, name.to_owned()));
        }
        Ok(by_instance.into_values().collect())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_sources_deduplicates_shared_instance_and_keeps_separate_instances() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database_config.json");
        fs::write(
            &path,
            r#"{
                "INSTANCES": {
                    "redis": {"hostname":"127.0.0.1", "port":6379,
                              "unix_socket_path":"/run/redis.sock"},
                    "redis2": {"hostname":"127.0.0.1", "port":63792,
                               "unix_socket_path":"/run/redis2.sock"}
                },
                "DATABASES": {
                    "CONFIG_DB":{"id":4,"instance":"redis"},
                    "ASIC_DB":{"id":1,"instance":"redis2"},
                    "APPL_DB":{"id":0,"instance":"redis2"}
                }
            }"#,
        )
        .unwrap();
        let layout = Layout::load(&path).unwrap();
        let sources = layout.monitor_sources().unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(
            sources
                .iter()
                .map(|source| source.databases.clone())
                .collect::<Vec<_>>(),
            vec![
                BTreeMap::from([(4, "CONFIG_DB".to_owned())]),
                BTreeMap::from([(0, "APPL_DB".to_owned()), (1, "ASIC_DB".to_owned())])
            ]
        );
    }

    #[test]
    fn test_monitor_sources_requires_all_pipeline_databases() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database_config.json");
        fs::write(
            &path,
            r#"{"INSTANCES":{"redis":{}},"DATABASES":{"CONFIG_DB":{"id":4,"instance":"redis"}}}"#,
        )
        .unwrap();
        let layout = Layout::load(&path).unwrap();
        assert_eq!(
            layout.monitor_sources().unwrap_err().to_string(),
            "APPL_DB is missing from database_config.json"
        );
    }
}
