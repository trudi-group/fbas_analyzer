use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn multiple_merging_options_passed() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("fbas_analyzer")?;
    cmd.arg("test_data/stellarbeat_nodes_2019-09-17.json")
        .arg("--merge-by-isp")
        .arg("--merge-by-country")
        .arg("--merge-by-org")
        .arg("test_data/stellarbeat_organizations_2019-09-17.json")
        .arg("-p");
    cmd.assert().success().stderr(predicate::str::contains(
        "Multiple merging options detected; will only merge nodes by country...",
    ));
    Ok(())
}

#[test]
fn json_describing_fbas_not_available_as_file() -> Result<(), Box<dyn std::error::Error>> {
    let fbas_input = r#"[
            {
                "publicKey": "Jim",
                "geoData": {
                    "countryName": "Oceania,"
                }
            },
            {
                "publicKey": "Jon",
                "geoData": {
                    "countryName": "Oceania"
                }
            },
            {
                "publicKey": "Alex",
                "geoData": {
                    "countryName": "Eastasia"
                }
            },
            {
                "publicKey": "Bob"
            }
            ]"#;
    Command::cargo_bin("fbas_analyzer")?
        .write_stdin(fbas_input.as_bytes())
        .arg("--merge-by-country")
        .arg("-p")
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Will not merge. JSON file describing FBAS needed to perform merge.",
        ));
    Ok(())
}

#[test]
fn merge_by_ctry_cli_arg_works() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("fbas_analyzer")?;
    cmd.arg("test_data/stellarbeat_nodes_2019-09-17.json")
        .arg("--merge-by-country")
        .arg("-q")
        .arg("-p")
        .arg("--results-only");
    cmd.assert().success().stdout(predicate::str::contains(
        r#"top_tier: ["United States","Finland","Germany"]"#,
    ));
    Ok(())
}
