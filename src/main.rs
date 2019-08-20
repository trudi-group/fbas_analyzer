extern crate env_logger;
extern crate fba_quorum_analyzer;

use std::env;

use fba_quorum_analyzer::*;

fn main() {
    env_logger::init();

    if let Some(path) = env::args().nth(1) {
        let network = Network::from_json_file(&path);

        println!(
            "(In all following dumps, nodes are identified by their index in the input JSON.)\n"
        );

        let minimal_quorums = get_minimal_quorums(&network);
        println!("We found {} minimal quorums:", minimal_quorums.len());
        println!("\n{}\n", node_sets_to_json(&minimal_quorums));

        let minimal_blocking_sets = get_minimal_blocking_sets(&minimal_quorums);
        println!(
            "We found {} minimal blocking sets:",
            minimal_blocking_sets.len()
        );
        println!("\n{}\n", node_sets_to_json(&minimal_blocking_sets));

        println!(
            "Control over any of these node sets is sufficient to compromise liveliness and \
             censor future transactions.\n"
        );

        if all_node_sets_interesect(&minimal_quorums) {
            println!("All quorums intersect.");
        } else {
            println!("Some quorums don't intersect - safety severely threatened for some nodes!");
        }
        println!();
    } else {
        println!(
            "Usage: {} path-to-stellarbeat-nodes.json",
            env::args().next().unwrap()
        );
    }
}
