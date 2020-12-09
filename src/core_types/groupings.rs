use super::*;

/// Defines one concrete way to group the nodes of an `Fbas`, for example by organization, ISP or
/// country. Used for merging nodes belonging to the same group into one.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Groupings<'fbas> {
    pub(crate) groupings: Vec<Grouping>,
    pub(crate) merged_ids: Vec<NodeId>,
    node_id_to_org_idx: HashMap<NodeId, usize>,
    // for ensuring fbas remains stable + serializeability via Serialize trait
    pub(crate) fbas: &'fbas Fbas,
}
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Grouping {
    pub(crate) name: String,
    pub(crate) validators: Vec<NodeId>,
}
impl<'fbas> Groupings<'fbas> {
    pub(crate) fn new(groupings: Vec<Grouping>, fbas: &'fbas Fbas) -> Self {
        let mut merged_ids: Vec<NodeId> = (0..fbas.nodes.len()).collect();
        let mut node_id_to_org_idx: HashMap<NodeId, usize> = HashMap::new();

        for (org_idx, org) in groupings.iter().enumerate() {
            let mut validator_it = org.validators.iter().copied();
            if let Some(merged_id) = validator_it.next() {
                node_id_to_org_idx.insert(merged_id, org_idx);
                for validator in validator_it {
                    merged_ids[validator] = merged_id;
                    node_id_to_org_idx.insert(validator, org_idx);
                }
            }
        }
        Groupings {
            groupings,
            merged_ids,
            node_id_to_org_idx,
            fbas,
        }
    }
    pub fn get_by_member(&self, node_id: NodeId) -> Option<&Grouping> {
        if let Some(&org_idx) = self.node_id_to_org_idx.get(&node_id) {
            Some(&self.groupings[org_idx])
        } else {
            None
        }
    }
    pub fn get_by_name(&self, name: &str) -> Option<&Grouping> {
        self.groupings.iter().find(|org| org.name == name)
    }
    pub fn number_of_groupings(&self) -> usize {
        self.groupings.len()
    }
}
