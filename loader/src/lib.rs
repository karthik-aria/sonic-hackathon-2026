//! Loads pipelines from upstream `SONiC` YANG models plus a YAML file of ours.
//!
//! The YANG files are never edited. They supply the CONFIG table (the list's parent container),
//! the key leaves and the field names in schema order; the YAML supplies what YANG does not
//! describe (the APPL table, the SAI object types, the name rule and any leaf the producer does
//! not forward).

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use config_analyzer_pipeline_manager::pipeline::{NameRule, Pipeline};
use serde::Deserialize;
use yang3::context::{Context, ContextFlags};
use yang3::schema::{SchemaNode, SchemaNodeKind};

// The coverage mask in `Instance::uncovered` is a `u64`.
const MAX_FIELDS: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read {path}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse {path}")]
    Yaml {
        path: PathBuf,
        source: Box<serde_saphyr::Error>,
    },
    #[error("failed to load YANG: {0}")]
    Yang(#[from] yang3::Error),
    #[error("{path} is not a list")]
    NotAList { path: String },
    #[error("{path} is not inside a container, so there is no CONFIG table")]
    NoConfigTable { path: String },
    #[error("pipeline {pipeline}: {leaf} is not a leaf of {list}")]
    UnknownLeaf {
        pipeline: String,
        leaf: String,
        list: String,
    },
    #[error("pipeline {pipeline}: {count} fields, at most 64")]
    TooManyFields { pipeline: String, count: usize },
    #[error("table {table} is used by more than one pipeline")]
    DuplicateTable { table: String },
}

#[derive(Debug, Deserialize)]
struct Spec {
    pipelines: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
struct Entry {
    name: String,
    yang: Yang,
    appl_table: String,
    #[serde(default)]
    not_forwarded: Vec<String>,
    #[serde(default)]
    asic_objects: Vec<String>,
    object_names: Names,
}

#[derive(Debug, Deserialize)]
struct Yang {
    module: String,
    list: String,
}

#[derive(Debug, Deserialize)]
struct Names {
    prefix: String,
    digits: bool,
}

/// Reads `pipelines_yaml` and the YANG models in `yang_dir` and returns one `Pipeline` per entry.
///
/// The pipelines are leaked: they are built once at startup and live until the process exits,
/// which keeps `Pipeline` and everything derived from it free of lifetimes.
#[inline]
pub fn load(yang_dir: &Path, pipelines_yaml: &Path) -> Result<Vec<&'static Pipeline>, LoadError> {
    let text = fs::read_to_string(pipelines_yaml).map_err(|source| LoadError::Read {
        path: pipelines_yaml.to_owned(),
        source,
    })?;
    let spec: Spec = serde_saphyr::from_str(&text).map_err(|source| LoadError::Yaml {
        path: pipelines_yaml.to_owned(),
        source: Box::new(source),
    })?;
    let mut context = Context::new(ContextFlags::REF_IMPLEMENTED)?;
    context.set_searchdir(yang_dir)?;
    let mut tables: HashSet<&'static str> = HashSet::new();
    let mut pipelines: Vec<&'static Pipeline> = Vec::with_capacity(spec.pipelines.len());
    for entry in &spec.pipelines {
        let pipeline = build(&mut context, entry)?;
        // The router maps a CONFIG table and an APPL table to one pipeline each.
        for table in [pipeline.config_table, Some(pipeline.appl_table)]
            .into_iter()
            .flatten()
        {
            if !tables.insert(table) {
                return Err(LoadError::DuplicateTable {
                    table: table.to_owned(),
                });
            }
        }
        pipelines.push(Box::leak(Box::new(pipeline)));
    }
    Ok(pipelines)
}

// Reads one entry against the loaded YANG models. `context` must already hold `entry.yang.module`.
fn build(context: &mut Context, entry: &Entry) -> Result<Pipeline, LoadError> {
    // Loading the module is what makes its nodes reachable; the handle itself is not needed.
    let _module = context.load_module(&entry.yang.module, None, &[])?;
    let list = context.find_path(&entry.yang.list)?;
    if list.kind() != SchemaNodeKind::List {
        return Err(LoadError::NotAList {
            path: entry.yang.list.clone(),
        });
    }
    let config_table = list
        .ancestors()
        .next()
        .map(|container| container.name().to_owned())
        .ok_or_else(|| LoadError::NoConfigTable {
            path: entry.yang.list.clone(),
        })?;
    let keys: Vec<String> = list.list_keys().map(|key| key.name().to_owned()).collect();
    let mut leaves = Vec::new();
    collect_leaves(&list, &mut leaves);
    for leaf in &entry.not_forwarded {
        if !leaves.iter().any(|(name, _)| name == leaf) {
            return Err(LoadError::UnknownLeaf {
                pipeline: entry.name.clone(),
                leaf: leaf.clone(),
                list: entry.yang.list.clone(),
            });
        }
    }
    // Key leaves are the key of the APPL table entry, not fields of it. A leaf-list arrives in
    // CONFIG_DB under `<name>@` and in APPL_DB under `<name>`, hence the alias.
    let mut fields = Vec::new();
    let mut field_alias = Vec::new();
    for (name, is_leaf_list) in leaves {
        if keys.contains(&name) || entry.not_forwarded.contains(&name) {
            continue;
        }
        if is_leaf_list {
            field_alias.push((format!("{name}@"), name.clone()));
        }
        fields.push(name);
    }
    if fields.len() > MAX_FIELDS {
        return Err(LoadError::TooManyFields {
            pipeline: entry.name.clone(),
            count: fields.len(),
        });
    }
    Ok(Pipeline {
        name: leak_str(&entry.name),
        config_table: Some(leak_str(&config_table)),
        appl_table: leak_str(&entry.appl_table),
        fields: leak_names(&fields),
        field_alias: leak_alias(&field_alias),
        asic_slots: leak_names(&entry.asic_objects),
        object_names: NameRule {
            prefix: leak_str(&entry.object_names.prefix),
            digits: entry.object_names.digits,
        },
    })
}

fn leak_str(value: &str) -> &'static str {
    Box::leak(value.to_owned().into_boxed_str())
}

fn leak_names(names: &[String]) -> &'static [&'static str] {
    Box::leak(
        names
            .iter()
            .map(|name| leak_str(name))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    )
}

fn leak_alias(alias: &[(String, String)]) -> &'static [(&'static str, &'static str)] {
    Box::leak(
        alias
            .iter()
            .map(|(config, appl)| (leak_str(config), leak_str(appl)))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    )
}

// Leaves of `node` in schema order, descending into `choice` / `case`. Nested containers and
// lists are separate tables and are not fields of this one.
fn collect_leaves(node: &SchemaNode<'_>, out: &mut Vec<(String, bool)>) {
    for child in node.children() {
        match child.kind() {
            SchemaNodeKind::Leaf => out.push((child.name().to_owned(), false)),
            SchemaNodeKind::LeafList => out.push((child.name().to_owned(), true)),
            SchemaNodeKind::Choice | SchemaNodeKind::Case => collect_leaves(&child, out),
            SchemaNodeKind::Container
            | SchemaNodeKind::List
            | SchemaNodeKind::AnyXml
            | SchemaNodeKind::AnyData
            | SchemaNodeKind::Rpc
            | SchemaNodeKind::Input
            | SchemaNodeKind::Output
            | SchemaNodeKind::Action
            | SchemaNodeKind::Notification => {}
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::unwrap_in_result, reason = "test code")]
mod tests {
    use super::*;
    use std::fmt::Write;

    // One container, one list, one key, two leaves and one leaf-list.
    const TEST_YANG: &str = r#"
module test {
  yang-version 1.1;
  namespace "http://example.com/test";
  prefix test;

  container sonic-test {
    container TABLE {
      list TABLE_LIST {
        key "name";
        leaf name { type string; }
        leaf a { type string; }
        leaf b { type string; }
        leaf-list c { type string; }
      }
    }
  }
}
"#;

    const TEST_YAML: &str = "
pipelines:
  - name: test
    yang:
      module: test
      list: /test:sonic-test/TABLE/TABLE_LIST
    appl_table: TABLE_TABLE
    not_forwarded: [b]
    asic_objects: [SAI_OBJECT_TYPE_TEST]
    object_names:
      prefix: Test
      digits: true
";

    // A module whose list is at the top level, so it has no CONFIG table.
    const TOP_LIST_YANG: &str = r#"
module test {
  yang-version 1.1;
  namespace "http://example.com/test";
  prefix test;

  list TABLE_LIST {
    key "name";
    leaf name { type string; }
  }
}
"#;

    const TOP_LIST_YAML: &str = "
pipelines:
  - name: test
    yang:
      module: test
      list: /test:TABLE_LIST
    appl_table: TABLE_TABLE
    object_names:
      prefix: Test
      digits: true
";

    // A module with more leaves than the coverage mask has bits.
    fn wide_yang(count: usize) -> String {
        let mut leaves = String::new();
        for index in 0..count {
            writeln!(leaves, "        leaf f{index} {{ type string; }}").unwrap();
        }
        format!(
            r#"module test {{
  yang-version 1.1;
  namespace "http://example.com/test";
  prefix test;

  container sonic-test {{
    container TABLE {{
      list TABLE_LIST {{
        key "name";
        leaf name {{ type string; }}
{leaves}      }}
    }}
  }}
}}
"#
        )
    }

    fn load_fixture(yang: &str, yaml: &str) -> Result<Vec<&'static Pipeline>, LoadError> {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("test.yang"), yang).unwrap();
        let path = dir.path().join("pipelines.yaml");
        fs::write(&path, yaml).unwrap();
        load(dir.path(), &path)
    }

    fn fixture(yaml: &str) -> Result<Vec<&'static Pipeline>, LoadError> {
        load_fixture(TEST_YANG, yaml)
    }

    // The key is not a field, `not_forwarded` removes `b`, and the leaf-list gets its alias.
    #[test]
    fn test_load_fixture() {
        let pipelines = fixture(TEST_YAML).unwrap();
        assert_eq!(pipelines.len(), 1);
        let test = pipelines.first().unwrap();
        assert_eq!(
            (
                test.name,
                test.config_table,
                test.appl_table,
                test.fields.to_vec(),
                test.field_alias.to_vec(),
                test.asic_slots.to_vec(),
                test.object_names,
            ),
            (
                "test",
                Some("TABLE"),
                "TABLE_TABLE",
                vec!["a", "c"],
                vec![("c@", "c")],
                vec!["SAI_OBJECT_TYPE_TEST"],
                NameRule {
                    prefix: "Test",
                    digits: true,
                },
            )
        );
    }

    #[test]
    fn test_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.yaml");
        assert_eq!(
            load(dir.path(), &path).unwrap_err().to_string(),
            format!("failed to read {}", path.display())
        );
    }

    #[test]
    fn test_load_bad_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipelines.yaml");
        fs::write(&path, "pipelines: [").unwrap();
        assert_eq!(
            load(dir.path(), &path).unwrap_err().to_string(),
            format!("failed to parse {}", path.display())
        );
    }

    #[test]
    fn test_load_missing_module() {
        let yaml = TEST_YAML.replace("module: test", "module: nope");
        assert!(matches!(fixture(&yaml), Err(LoadError::Yang(_))));
    }

    #[test]
    fn test_load_not_a_list() {
        let yaml = TEST_YAML.replace(
            "/test:sonic-test/TABLE/TABLE_LIST",
            "/test:sonic-test/TABLE",
        );
        assert_eq!(
            fixture(&yaml).unwrap_err().to_string(),
            "/test:sonic-test/TABLE is not a list"
        );
    }

    #[test]
    fn test_load_no_config_table() {
        let err = load_fixture(TOP_LIST_YANG, TOP_LIST_YAML).unwrap_err();
        assert_eq!(
            err.to_string(),
            "/test:TABLE_LIST is not inside a container, so there is no CONFIG table"
        );
    }

    #[test]
    fn test_load_unknown_leaf() {
        let yaml = TEST_YAML.replace("not_forwarded: [b]", "not_forwarded: [zzz]");
        assert_eq!(
            fixture(&yaml).unwrap_err().to_string(),
            "pipeline test: zzz is not a leaf of /test:sonic-test/TABLE/TABLE_LIST"
        );
    }

    #[test]
    fn test_load_too_many_fields() {
        let yaml = TEST_YAML.replace("not_forwarded: [b]", "not_forwarded: []");
        let err = load_fixture(&wide_yang(MAX_FIELDS + 1), &yaml).unwrap_err();
        assert_eq!(err.to_string(), "pipeline test: 65 fields, at most 64");
    }

    #[test]
    fn test_load_duplicate_table() {
        let yaml = "
pipelines:
  - name: test
    yang:
      module: test
      list: /test:sonic-test/TABLE/TABLE_LIST
    appl_table: TABLE_TABLE
    object_names:
      prefix: Test
      digits: true
  - name: test2
    yang:
      module: test
      list: /test:sonic-test/TABLE/TABLE_LIST
    appl_table: TABLE_TABLE
    object_names:
      prefix: Other
      digits: true
";
        assert_eq!(
            fixture(yaml).unwrap_err().to_string(),
            "table TABLE is used by more than one pipeline"
        );
    }
}
