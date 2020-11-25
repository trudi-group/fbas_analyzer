use super::*;

#[derive(Serialize, Deserialize)]
struct RawGroupings(Vec<RawGrouping>);
#[derive(Serialize, Deserialize)]
struct RawGrouping {
    name: String,
    validators: Vec<PublicKey>,
}
impl<'fbas> Groupings<'fbas> {
    pub fn from_json_str(json: &str, fbas: &'fbas Fbas) -> Self {
        Self::from_raw(
            serde_json::from_str(json).expect("Error parsing Organizations JSON"),
            fbas,
        )
    }
    pub fn from_json_file(path: &Path, fbas: &'fbas Fbas) -> Self {
        let json =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("Error reading file {:?}", path));
        Self::from_json_str(&json, fbas)
    }
    fn from_raw(raw_groupings: RawGroupings, fbas: &'fbas Fbas) -> Self {
        let groupings: Vec<Grouping> = raw_groupings
            .0
            .into_iter()
            .map(|x| Grouping::from_raw(x, &fbas.pk_to_id))
            .collect();

        Groupings::new(groupings, fbas)
    }
    fn to_raw(&self) -> RawGroupings {
        RawGroupings(
            self.groupings
                .iter()
                .map(|org| org.to_raw(self.fbas))
                .collect(),
        )
    }
}
impl<'fbas> Serialize for Groupings<'fbas> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_raw().serialize(serializer)
    }
}
impl Grouping {
    fn from_raw(raw_grouping: RawGrouping, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Grouping {
            name: raw_grouping.name,
            validators: raw_grouping
                .validators
                .into_iter()
                .filter_map(|pk| pk_to_id.get(&pk))
                .cloned()
                .collect(),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawGrouping {
        RawGrouping {
            name: self.name.clone(),
            validators: self
                .validators
                .iter()
                .map(|&x| fbas.nodes[x].public_key.clone())
                .collect(),
        }
    }
}
