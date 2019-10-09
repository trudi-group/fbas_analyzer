pub use bit_set::BitSet;
pub use std::collections::HashMap;
pub use std::collections::VecDeque;

pub type NodeId = usize; // internal and possibly different between runs
pub type PublicKey = String;

pub type NodeIdSet = BitSet;
pub type NodeIdDeque = VecDeque<NodeId>;

#[derive(Debug, PartialEq)]
pub struct Fbas {
    pub(crate) nodes: Vec<Node>,
    pub(crate) pk_to_id: HashMap<PublicKey, NodeId>,
}

#[derive(Debug, PartialEq)]
pub struct Node {
    pub(crate) public_key: PublicKey,
    pub(crate) quorum_set: QuorumSet,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QuorumSet {
    pub(crate) threshold: usize,
    pub(crate) validators: Vec<NodeId>,
    pub(crate) inner_quorum_sets: Vec<QuorumSet>,
}

pub struct Organizations {
    pub(crate) organizations: Vec<Organization>,
    pub(crate) collapsed_ids: Vec<NodeId>,
    id_to_org_idx: HashMap<NodeId, usize>,
}
pub struct Organization {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) validators: Vec<NodeId>,
}
impl Organizations {
    pub fn new(organizations: Vec<Organization>, fbas: &Fbas) -> Self {
        let mut collapsed_ids: Vec<NodeId> = (0..fbas.nodes.len()).collect();
        let mut id_to_org_idx: HashMap<NodeId, usize> = HashMap::new();

        for (org_idx, org) in organizations.iter().enumerate() {
            let mut validator_it = org.validators.iter().copied();
            if let Some(collapsed_id) = validator_it.next() {
                id_to_org_idx.insert(collapsed_id, org_idx);
                for validator in validator_it {
                    collapsed_ids[validator] = collapsed_id;
                    id_to_org_idx.insert(validator, org_idx);
                }
            }
        }
        Organizations {
            organizations,
            collapsed_ids,
            id_to_org_idx,
        }
    }
    pub fn get_by_member(self: &Self, node_id: NodeId) -> Option<&Organization> {
        if let Some(&org_idx) = self.id_to_org_idx.get(&node_id) {
            Some(&self.organizations[org_idx])
        } else {
            None
        }
    }
}

/// Create a **BitSet** from a list of elements.
///
/// ## Example
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
///
/// let set = bitset!{23, 42};
/// assert!(set.contains(23));
/// assert!(set.contains(42));
/// assert!(!set.contains(100));
/// ```
#[macro_export]
macro_rules! bitset {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(bitset!(@single $rest)),*]));

    ($($key:expr,)+) => { bitset!($($key),+) };
    ($($key:expr),*) => {
        {
            let _cap = bitset!(@count $($key),*);
            let mut _set = ::bit_set::BitSet::with_capacity(_cap);
            $(
                let _ = _set.insert($key);
            )*
            _set
        }
    };
}
