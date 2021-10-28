use super::*;
use serde_json::Value;

/// The result of filtering nodes by a predicate. Transform `into_pretty_vec` for usage in one of
/// several `without_nodes` functions.
/// ```
/// use fbas_analyzer::FilteredNodes;
///
/// let input = r#"[
///     {
///         "publicKey": "Alice",
///         "active": true
///     },
///     {
///         "publicKey": "Bob",
///         "active": false
///     },
///     {
///         "publicKey": "Charlie"
///     }
/// ]"#;
/// let inactive_nodes = FilteredNodes::from_json_str(&input, |v| v["active"] == false);
/// assert_eq!(vec!["Bob"], inactive_nodes.into_pretty_vec());
/// ```
pub struct FilteredNodes(Vec<PublicKey>);

impl FilteredNodes {
    pub fn from_json_str<P>(json: &str, mut predicate: P) -> Self
    where
        P: FnMut(&Value) -> bool,
    {
        let mut nodes = vec![];
        if let Ok(Value::Array(values)) = serde_json::from_str::<Value>(json) {
            for value in values.into_iter() {
                if predicate(&value) {
                    nodes.push(
                        value["publicKey"]
                            .as_str()
                            .expect("Node without publicKey!")
                            .into(),
                    );
                }
            }
        }
        Self(nodes)
    }
    pub fn from_json_file<P>(path: &Path, predicate: P) -> Self
    where
        P: FnMut(&Value) -> bool,
    {
        Self::from_json_str(&read_or_panic!(path), predicate)
    }
    pub fn into_pretty_vec(self) -> Vec<PublicKey> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_nodes_from_json() {
        let path = Path::new("test_data/stellarbeat_nodes_2019-09-17.json");
        let predicate = |v: &Value| v["active"] == false;

        let mut nodes = FilteredNodes::from_json_file(path, predicate).into_pretty_vec();
        nodes.sort_unstable();
        nodes.truncate(5);

        let actual = nodes;
        let expected = vec![
            "GA2AV42B6W4HO3M36RMZKEHY36B3K3W4AMMAYLSVGZWUPZUEU4XGAX6R",
            "GA2XJSKK4EETH5H56RQCICRSEZ3KOQNUASQOQN2DKMW7APH3VBBZOOH2",
            "GA5UB6D64SV4OLDNCHJJH7YZT2IGKVCPNJVMG5EX5FOIM7WFYRSJLV7B",
            "GA6C6E7SM7OJCW3MRHPY2KG7KTJDXFHERG5IN4JUMXNYQJLBHCFK4QUR",
            "GA6HXDHPLGE5E7DD6CF5ZZ3KEOGPTCGBQL5N27XBIN36UTN6NDJYJYGV",
        ];
        assert_eq!(expected, actual);
    }
}
