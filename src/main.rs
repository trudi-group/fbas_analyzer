extern crate env_logger;
extern crate fba_quorum_analyzer;

use std::env;

use fba_quorum_analyzer::*;

fn main() {
    env_logger::init();

    if let Some(path) = env::args().nth(1) {
        let network = Network::from_json_file(&path);

        let minimal_quorums = get_minimal_quorums(&network);
        let intersection = all_node_sets_interesect(&minimal_quorums);

        let smallest_minimal_quorum_size = minimal_quorums.iter().map(|x| x.len()).min().unwrap();
        let smallest_minimal_quorums_count = minimal_quorums
            .iter()
            .filter(|x| x.len() <= smallest_minimal_quorum_size)
            .count();

        println!(
            "We found {} minimal quorums ({} of them with size {}, the smallest quorum size).",
            minimal_quorums.len(),
            smallest_minimal_quorums_count,
            smallest_minimal_quorum_size
        );
        if intersection {
            println!("Each of these node groups can effectively censor future transactions.");
        }
        println!("");

        if intersection {
            println!("All quorums intersect.");
        } else {
            println!("Some quorums don't intersect - safety severely threatened for some nodes!");
        }
        println!("");

        println!("Here is a dump of all minimal quorums (nodes are identified by their index in the input JSON):");
        println!("{}", node_sets_to_json(&minimal_quorums));
    } else {
        println!(
            "Usage: {} path-to-stellarbeat-nodes.json",
            env::args().next().unwrap()
        );
    }
}
