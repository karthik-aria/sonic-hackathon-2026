use std::sync::Arc;

pub type PipelineIdx = usize;
pub type InstanceId = usize;
pub type SlotIdx = usize;

// Which ObjectMap names belong to a pipeline: `prefix` followed by, when `digits`, one or more
// ASCII digits. Data rather than a function so it can be loaded from configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NameRule {
    pub prefix: &'static str,
    pub digits: bool,
}

impl NameRule {
    #[must_use]
    #[inline]
    pub fn matches(&self, name: &str) -> bool {
        name.strip_prefix(self.prefix).is_some_and(|rest| {
            !self.digits || (!rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
        })
    }
}

// Shared, read-only. One per flow type; the process leaks one per loaded pipeline at startup.
#[derive(Debug)]
pub struct Pipeline {
    pub name: &'static str,
    pub config_table: Option<&'static str>,
    pub appl_table: &'static str,
    // Index = bit position in `Instance::uncovered`; at most 64 entries.
    pub fields: &'static [&'static str],
    // (CONFIG name, APPL name)
    pub field_alias: &'static [(&'static str, &'static str)],
    // SAI object types, index = SlotIdx.
    pub asic_slots: &'static [&'static str],
    // Which ObjectMap names belong to this pipeline.
    pub object_names: NameRule,
}

impl Pipeline {
    // Bit i is set for each name equal to fields[i] after mapping through field_alias.
    // Names not in `fields` are ignored.
    #[must_use]
    #[inline]
    pub fn field_mask(&self, names: &[Arc<str>]) -> u64 {
        names.iter().fold(0_u64, |mask, name| {
            let name = self
                .field_alias
                .iter()
                .find(|(config, _)| *config == name.as_ref())
                .map_or_else(|| name.as_ref(), |(_, appl)| *appl);
            self.fields
                .iter()
                .position(|field| *field == name)
                .and_then(|bit| u32::try_from(bit).ok())
                .and_then(|bit| 1_u64.checked_shl(bit))
                .map_or(mask, |bit| mask | bit)
        })
    }
}

pub use config_analyzer_core::{
    SPAN_APPL, SPAN_CONFIG, SPAN_CREATE, SPAN_REMOVE, SPAN_SET, TRACK_APPL, TRACK_CONFIG,
};

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    static TEST_PIPELINE: Pipeline = Pipeline {
        name: "test",
        config_table: None,
        appl_table: "TEST_TABLE",
        fields: &["speed", "mode"],
        field_alias: &[("speed@", "speed")],
        asic_slots: &[],
        object_names: NameRule {
            prefix: "Object",
            digits: true,
        },
    };

    #[rstest]
    #[case::first_field(&["speed"], 1_u64)]
    #[case::alias(&["speed@"], 1_u64)]
    #[case::second_field(&["mode"], 2_u64)]
    #[case::both_fields(&["speed@", "mode"], 3_u64)]
    #[case::unknown(&["unknown"], 0_u64)]
    #[case::empty(&[], 0_u64)]
    fn test_field_mask(#[case] names: &[&str], #[case] expected: u64) {
        let names: Vec<Arc<str>> = names.iter().map(|name| Arc::from(*name)).collect();
        assert_eq!(TEST_PIPELINE.field_mask(&names), expected);
    }

    #[rstest]
    #[case("Object1", true)]
    #[case("Object120", true)]
    #[case("Object", false)]
    #[case("Object9:7", false)]
    #[case("Object1a", false)]
    #[case("object1", false)]
    #[case("PortChannel1", false)]
    #[case("Vlan100", false)]
    fn test_name_rule(#[case] name: &str, #[case] expected: bool) {
        assert_eq!(TEST_PIPELINE.object_names.matches(name), expected);
    }

    #[rstest]
    #[case("", false, "", true)]
    #[case("", false, "Vlan100", true)]
    #[case("Vlan", false, "Vlan100", true)]
    #[case("Vlan", false, "Ethernet1", false)]
    #[case("Vlan", true, "Vlan100", true)]
    #[case("Vlan", true, "Vlan100:1", false)]
    #[case("Vlan", true, "Vlan", false)]
    #[case("Vlan", true, "PortChannel1", false)]
    fn test_name_rule_free_form(
        #[case] prefix: &'static str,
        #[case] digits: bool,
        #[case] name: &str,
        #[case] expected: bool,
    ) {
        let rule = NameRule { prefix, digits };
        assert_eq!(rule.matches(name), expected);
    }
}
