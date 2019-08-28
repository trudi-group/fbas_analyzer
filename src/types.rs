pub type NodeId = usize; // internal and possibly different between runs
pub type PublicKey = String;

#[derive(Debug, PartialEq)]
pub struct Network {
    pub(crate) nodes: Vec<Node>,
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
