extern crate fba_quorum_analyzer;

use std::env;

use fba_quorum_analyzer::*;

fn main() {
    if let Some(path) = env::args().nth(1) {
        let network = Network::from_json_file(&path);

        let minimal_quorums = get_minimal_quorums(&network);

        println!("{:?}", minimal_quorums);

        if all_node_sets_interesect(&minimal_quorums) {
            println!("All quorums intersect.");
        } else {
            println!("Don't intersect...");
        }
    } else {
        println!(
            "Usage: {} path-to-stellarbeat-nodes.json",
            env::args().next().unwrap()
        );
    }
}
