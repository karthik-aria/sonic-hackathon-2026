use crate::pipeline::{NameRule, Pipeline};

pub(crate) const TEST_PORT_BASE: [&str; 12] = [
    "field0", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8",
    "field9", "field10", "field11",
];

pub(crate) const TEST_PORT_CONFIG_FIELDS: [&str; 14] = [
    "field0",
    "field1",
    "field2",
    "field3",
    "field4",
    "field5",
    "field6",
    "field7",
    "field8",
    "field9",
    "field10@",
    "field11",
    "mtu",
    "admin_status",
];

pub(crate) static TEST_PORT: Pipeline = Pipeline {
    name: "port",
    config_table: Some("PORT"),
    appl_table: "PORT_TABLE",
    fields: &[
        "field0",
        "field1",
        "field2",
        "field3",
        "field4",
        "field5",
        "field6",
        "field7",
        "field8",
        "field9",
        "field10",
        "field11",
        "mtu",
        "admin_status",
    ],
    field_alias: &[("field10@", "field10")],
    asic_slots: &["SAI_OBJECT_TYPE_PORT", "SAI_OBJECT_TYPE_ROUTER_INTERFACE"],
    object_names: NameRule {
        prefix: "Ethernet",
        digits: true,
    },
};
