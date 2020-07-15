use super::*;

#[derive(Serialize, Deserialize)]
struct RawOrganizations(Vec<RawOrganization>);
#[derive(Serialize, Deserialize)]
struct RawOrganization {
    name: String,
    validators: Vec<PublicKey>,
}
impl<'fbas> Organizations<'fbas> {
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
    fn from_raw(raw_organizations: RawOrganizations, fbas: &'fbas Fbas) -> Self {
        let organizations: Vec<Organization> = raw_organizations
            .0
            .into_iter()
            .map(|x| Organization::from_raw(x, &fbas.pk_to_id))
            .collect();

        Organizations::new(organizations, fbas)
    }
    fn to_raw(&self) -> RawOrganizations {
        RawOrganizations(
            self.organizations
                .iter()
                .map(|org| org.to_raw(self.fbas))
                .collect(),
        )
    }
}
impl<'fbas> Serialize for Organizations<'fbas> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_raw().serialize(serializer)
    }
}
impl Organization {
    fn from_raw(raw_organization: RawOrganization, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Organization {
            name: raw_organization.name,
            validators: raw_organization
                .validators
                .into_iter()
                .filter_map(|pk| pk_to_id.get(&pk))
                .cloned()
                .collect(),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawOrganization {
        RawOrganization {
            name: self.name.clone(),
            validators: self
                .validators
                .iter()
                .map(|&x| fbas.nodes[x].public_key.clone())
                .collect(),
        }
    }
}
