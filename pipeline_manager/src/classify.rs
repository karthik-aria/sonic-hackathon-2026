use crate::{Db, RedisOp};
use std::sync::Arc;

const ASIC_QUEUE_KEY: &str = "ASIC_STATE_KEY_VALUE_OP_QUEUE";
const ASIC_STATE_PREFIX: &str = "ASIC_STATE:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    ConfigWritten {
        table: Arc<str>,
        key: Arc<str>,
        fields: Vec<Arc<str>>,
    },
    ApplQueued {
        table: Arc<str>,
        key: Arc<str>,
        fields: Vec<Arc<str>>,
    },
    ApplConsumed {
        table: Arc<str>,
        key: Arc<str>,
    },
    SaiRequested {
        obj_type: Arc<str>,
        oid: Arc<str>,
        op: SaiOp,
        attr: Option<Arc<str>>,
    },
    AsicWritten {
        obj_type: Arc<str>,
        oid: Arc<str>,
    },
    AsicDeleted {
        obj_type: Arc<str>,
        oid: Arc<str>,
    },
}

// "Screate" | "Sset" | "Sremove"
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaiOp {
    Create,
    Set,
    Remove,
}

#[must_use]
#[inline]
pub fn classify(op: &RedisOp) -> Option<Event> {
    let key = op.key.as_deref()?;
    match (op.db, op.cmd.as_ref()) {
        (Db::Config, "HSET" | "HMSET") => {
            let (table, key) = split(key, '|')?;
            Some(Event::ConfigWritten {
                table: Arc::from(table),
                key: Arc::from(key),
                fields: field_names(&op.args)?,
            })
        }
        (Db::Appl, "HSET" | "HMSET") => {
            let (table, key) = split(key.strip_prefix('_')?, ':')?;
            Some(Event::ApplQueued {
                table: Arc::from(table),
                key: Arc::from(key),
                fields: field_names(&op.args)?,
            })
        }
        (Db::Appl, "DEL") => {
            let (table, key) = split(key.strip_prefix('_')?, ':')?;
            Some(Event::ApplConsumed {
                table: Arc::from(table),
                key: Arc::from(key),
            })
        }
        (Db::Asic, "LPUSH") if key == ASIC_QUEUE_KEY => sai_request(&op.args),
        (Db::Asic, "HSET" | "HMSET") => {
            let (obj_type, oid) = split(key.strip_prefix(ASIC_STATE_PREFIX)?, ':')?;
            Some(Event::AsicWritten {
                obj_type: Arc::from(obj_type),
                oid: Arc::from(oid),
            })
        }
        (Db::Asic, "DEL") => {
            let (obj_type, oid) = split(key.strip_prefix(ASIC_STATE_PREFIX)?, ':')?;
            Some(Event::AsicDeleted {
                obj_type: Arc::from(obj_type),
                oid: Arc::from(oid),
            })
        }
        _ => None,
    }
}

fn split(s: &str, sep: char) -> Option<(&str, &str)> {
    s.split_once(sep)
        .filter(|(a, b)| !a.is_empty() && !b.is_empty())
}

fn field_names(args: &[Arc<str>]) -> Option<Vec<Arc<str>>> {
    (!args.is_empty() && args.len().is_multiple_of(2))
        .then(|| args.iter().step_by(2).map(Arc::clone).collect())
}

fn first_attr(s: &str) -> Option<Arc<str>> {
    let (name, _) = s.strip_prefix("[\"")?.split_once('"')?;
    (!name.is_empty()).then(|| Arc::from(name))
}

fn sai_request(args: &[Arc<str>]) -> Option<Event> {
    let [target, attrs, op] = args else {
        return None;
    };
    let op = match op.as_ref() {
        "Sset" => SaiOp::Set,
        "Screate" => SaiOp::Create,
        "Sremove" => SaiOp::Remove,
        _ => return None,
    };
    let (obj_type, oid) = split(target, ':')?;
    Some(Event::SaiRequested {
        obj_type: Arc::from(obj_type),
        oid: Arc::from(oid),
        op,
        attr: first_attr(attrs),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_analyzer_core::At;
    use rstest::rstest;

    const PORT_KEY: &str = "PORT|Ethernet1";
    const APPL_KEY: &str = "_PORT_TABLE:Ethernet1";
    const QUEUE: &str = "ASIC_STATE_KEY_VALUE_OP_QUEUE";
    const P: &str = "SAI_OBJECT_TYPE_PORT";
    const R: &str = "SAI_OBJECT_TYPE_ROUTER_INTERFACE";
    const OP: &str = "oid:0x1000000000002";
    const OR: &str = "oid:0x60000000001fb";
    const TARGET: &str = "SAI_OBJECT_TYPE_PORT:oid:0x1000000000002";
    const MTU_ATTR: &str = "[\"SAI_PORT_ATTR_MTU\",\"9134\"]";
    const CONFIG14: [&str; 14] = [
        "alias",
        "index",
        "lanes",
        "speed",
        "dhcp_rate_limit",
        "autoneg",
        "fec",
        "link_training",
        "description",
        "fast_linkup",
        "adv_speeds@",
        "role",
        "mtu",
        "admin_status",
    ];

    fn arcs(items: &[&str]) -> Vec<Arc<str>> {
        items.iter().map(|item| Arc::from(*item)).collect()
    }

    fn op(db: Db, cmd: &str, key: Option<&str>, args: &[&str]) -> RedisOp {
        RedisOp {
            at: At {
                seq: 1,
                ts_ns: 1_000,
            },
            db,
            cmd: Arc::from(cmd),
            key: key.map(Arc::from),
            args: arcs(args),
            client: Arc::from("test"),
        }
    }

    fn config14_args() -> Vec<&'static str> {
        CONFIG14.iter().flat_map(|name| [*name, "v"]).collect()
    }

    fn sai(op: SaiOp, attr: Option<&str>) -> Event {
        Event::SaiRequested {
            obj_type: Arc::from(P),
            oid: Arc::from(OP),
            op,
            attr: attr.map(Arc::from),
        }
    }

    #[rstest]
    #[case::config_hmset(
        op(Db::Config, "HMSET", Some(PORT_KEY), &config14_args()),
        Some(Event::ConfigWritten { table: Arc::from("PORT"), key: Arc::from("Ethernet1"), fields: arcs(&CONFIG14) })
    )]
    #[case::config_hset(
        op(Db::Config, "HSET", Some(PORT_KEY), &["mtu", "9215"]),
        Some(Event::ConfigWritten { table: Arc::from("PORT"), key: Arc::from("Ethernet1"), fields: arcs(&["mtu"]) })
    )]
    #[case::appl_hset(
        op(Db::Appl, "HSET", Some(APPL_KEY), &["mtu", "9112"]),
        Some(Event::ApplQueued { table: Arc::from("PORT_TABLE"), key: Arc::from("Ethernet1"), fields: arcs(&["mtu"]) })
    )]
    #[case::appl_del(
        op(Db::Appl, "DEL", Some(APPL_KEY), &[]),
        Some(Event::ApplConsumed { table: Arc::from("PORT_TABLE"), key: Arc::from("Ethernet1") })
    )]
    #[case::sai_set(
        op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, MTU_ATTR, "Sset"]),
        Some(sai(SaiOp::Set, Some("SAI_PORT_ATTR_MTU")))
    )]
    #[case::sai_create(
        op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, MTU_ATTR, "Screate"]),
        Some(sai(SaiOp::Create, Some("SAI_PORT_ATTR_MTU")))
    )]
    #[case::sai_remove(
        op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, MTU_ATTR, "Sremove"]),
        Some(sai(SaiOp::Remove, Some("SAI_PORT_ATTR_MTU")))
    )]
    #[case::sai_no_attr(
        op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, "[]", "Sset"]),
        Some(sai(SaiOp::Set, None))
    )]
    #[case::asic_written(
        op(
            Db::Asic,
            "HSET",
            Some("ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE:oid:0x60000000001fb"),
            &["SAI_ROUTER_INTERFACE_ATTR_MTU", "9112"],
        ),
        Some(Event::AsicWritten { obj_type: Arc::from(R), oid: Arc::from(OR) })
    )]
    #[case::asic_deleted(
        op(Db::Asic, "DEL", Some("ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x1000000000002"), &[]),
        Some(Event::AsicDeleted { obj_type: Arc::from(P), oid: Arc::from(OP) })
    )]
    #[case::appl_evalsha(op(Db::Appl, "EVALSHA", Some("c7faa1a7"), &["1"]), None)]
    #[case::appl_spop(op(Db::Appl, "SPOP", Some("PORT_TABLE_KEY_SET"), &["1024"]), None)]
    #[case::appl_sadd(op(Db::Appl, "SADD", Some("PORT_TABLE_KEY_SET"), &["Ethernet1"]), None)]
    #[case::appl_srem(op(Db::Appl, "SREM", Some("PORT_TABLE_DEL_SET"), &["Ethernet1"]), None)]
    #[case::appl_publish(op(Db::Appl, "PUBLISH", Some("PORT_TABLE_CHANNEL@0"), &["G"]), None)]
    #[case::appl_consumer_hset(op(Db::Appl, "HSET", Some("PORT_TABLE:Ethernet1"), &["mtu", "9112"]), None)]
    #[case::appl_del_no_prefix(op(Db::Appl, "DEL", Some("PORT_TABLE:Ethernet1"), &[]), None)]
    #[case::appl_lldp(op(Db::Appl, "HSET", Some("LLDP_PORT_TABLE:eth0"), &["port_id", "v"]), None)]
    #[case::appl_no_colon(op(Db::Appl, "HSET", Some("_PORT_TABLE"), &["mtu", "1"]), None)]
    #[case::appl_empty_table(op(Db::Appl, "HSET", Some("_:Ethernet1"), &["mtu", "1"]), None)]
    #[case::asic_getresponse(
        op(Db::Asic, "LPUSH", Some("GETRESPONSE_KEY_VALUE_OP_QUEUE"), &["SAI_STATUS_SUCCESS", "[]", "Sgetresponse"]),
        None
    )]
    #[case::sai_unknown_op(op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, "[]", "Sget"]), None)]
    #[case::sai_two_args(op(Db::Asic, "LPUSH", Some(QUEUE), &[TARGET, "[]"]), None)]
    #[case::asic_no_oid(op(Db::Asic, "HSET", Some("ASIC_STATE:SAI_OBJECT_TYPE_PORT"), &["a", "v"]), None)]
    #[case::config_set(op(Db::Config, "SET", Some("CONFIG_DB_UPDATED_PORT"), &["1"]), None)]
    #[case::config_evalsha(op(Db::Config, "EVALSHA", Some("eb4ffa"), &["1", "PORT|*"]), None)]
    #[case::config_no_pipe(op(Db::Config, "HSET", Some("PORT"), &["mtu", "1"]), None)]
    #[case::config_odd_args(op(Db::Config, "HSET", Some(PORT_KEY), &["mtu"]), None)]
    #[case::config_no_args(op(Db::Config, "HSET", Some(PORT_KEY), &[]), None)]
    #[case::other_db(op(Db::Other, "HSET", Some(APPL_KEY), &["mtu", "1"]), None)]
    #[case::no_key(op(Db::Config, "HSET", None, &["mtu", "1"]), None)]
    fn test_classify(#[case] input: RedisOp, #[case] expected: Option<Event>) {
        assert_eq!(classify(&input), expected);
    }

    #[rstest]
    #[case::mtu(MTU_ATTR, Some("SAI_PORT_ATTR_MTU"))]
    #[case::empty_list("[]", None)]
    #[case::empty_name("[\"\",\"v\"]", None)]
    #[case::garbage("garbage", None)]
    fn test_first_attr(#[case] input: &str, #[case] expected: Option<&str>) {
        assert_eq!(first_attr(input), expected.map(Arc::from));
    }
}
