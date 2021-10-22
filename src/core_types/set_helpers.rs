/// Create a `BitSet` from a list of elements.
///
/// ## Example
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

    () => { ::bit_set::BitSet::new() };
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

/// Create a `Vec<BitSet>` from a list of sets.
///
/// ## Example
/// ```
/// #[macro_use] extern crate fbas_analyzer;
///
/// let actual = bitsetvec![[0, 1], [23, 42]];
/// let expected = vec![bitset![0, 1], bitset![23, 42]];
/// assert_eq!(expected, actual);
/// ```
#[macro_export]
macro_rules! bitsetvec {
    ($($setcontent:tt),*) => {
        {
            vec![
            $(
                bitset!$setcontent
            ),*
            ]
        }
    };
}
