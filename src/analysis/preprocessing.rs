use super::*;

extern crate pathfinding;
use pathfinding::directed::strongly_connected_components::strongly_connected_components;

type RankScore = f64;

impl Fbas {
    pub fn satisfiable_nodes(&self) -> NodeIdSet {
        find_unsatisfiable_nodes(&self.all_nodes(), self).0
    }
    pub fn unsatisfiable_nodes(&self) -> NodeIdSet {
        find_unsatisfiable_nodes(&self.all_nodes(), self).1
    }
    pub fn strongly_connected_components(&self) -> Vec<NodeIdSet> {
        partition_into_strongly_connected_components(&self.all_nodes(), self)
    }
    pub fn rank_nodes(&self) -> Vec<RankScore> {
        let all_nodes: Vec<NodeId> = (0..self.nodes.len()).collect();
        rank_nodes(&all_nodes, self)
    }
}

/// Partitions `node_set` into the sets of `(satisfiable, unsatisfiable)' nodes.
pub fn find_unsatisfiable_nodes(node_set: &NodeIdSet, fbas: &Fbas) -> (NodeIdSet, NodeIdSet) {
    let (mut satisfiable, mut unsatisfiable): (NodeIdSet, NodeIdSet) = node_set
        .iter()
        .partition(|&x| fbas.nodes[x].is_quorum_slice(&node_set));

    while let Some(unsatisfiable_node) = satisfiable
        .iter()
        .find(|&x| !fbas.nodes[x].is_quorum_slice(&satisfiable))
    {
        satisfiable.remove(unsatisfiable_node);
        unsatisfiable.insert(unsatisfiable_node);
    }
    (satisfiable, unsatisfiable)
}

/// Using implementation from `pathfinding` crate.
pub fn partition_into_strongly_connected_components(
    nodes: &NodeIdSet,
    fbas: &Fbas,
) -> Vec<NodeIdSet> {
    let sucessors = |&node_id: &NodeId| -> Vec<NodeId> {
        fbas.nodes[node_id]
            .quorum_set
            .contained_nodes()
            .into_iter()
            .collect()
    };
    let nodes: Vec<NodeId> = nodes.iter().collect();

    let sccs = strongly_connected_components(&nodes, sucessors);
    sccs.into_iter().map(|x| x.into_iter().collect()).collect()
}

/// Returns the union of all strongly connected components with cardinality > 1
pub(crate) fn reduce_to_strongly_connected_nodes(
    mut nodes: NodeIdSet,
    fbas: &Fbas,
) -> (NodeIdSet, NodeIdSet) {
    let mut removed_nodes = nodes.clone();
    for node_id in nodes.iter() {
        let node = &fbas.nodes[node_id];
        for included_node in node.quorum_set.contained_nodes().into_iter() {
            if included_node == node_id {
                continue;
            }
            removed_nodes.remove(included_node);
        }
    }
    if !removed_nodes.is_empty() {
        nodes.difference_with(&removed_nodes);
        let (reduced_nodes, new_removed_nodes) = reduce_to_strongly_connected_nodes(nodes, fbas);
        nodes = reduced_nodes;
        removed_nodes.union_with(&new_removed_nodes);
    }
    (nodes, removed_nodes)
}

/// Rank nodes using an adaptation of the page rank algorithm (no dampening, fixed number of runs,
/// no distinction between validators and inner quorum set validators). Links from nodes not in
/// `nodes` are ignored.
// TODO dedup / harmonize this with Graph::get_rank_scores
pub fn rank_nodes(nodes: &[NodeId], fbas: &Fbas) -> Vec<RankScore> {
    let nodes_set: NodeIdSet = nodes.iter().cloned().collect();
    assert_eq!(nodes.len(), nodes_set.len());

    let runs = 100;
    let starting_score = 1. / nodes.len() as RankScore;

    let mut scores: Vec<RankScore> = vec![starting_score; fbas.nodes.len()];
    let mut last_scores: Vec<RankScore>;

    for _ in 0..runs {
        last_scores = scores;
        scores = vec![0.; fbas.nodes.len()];

        for node_id in nodes.iter().copied() {
            let node = &fbas.nodes[node_id];
            let trusted_nodes = node.quorum_set.contained_nodes();
            let l = trusted_nodes.len() as RankScore;

            for trusted_node_id in trusted_nodes
                .into_iter()
                .filter(|&id| nodes_set.contains(id))
            {
                scores[trusted_node_id] += last_scores[node_id] / l;
            }
        }
    }
    debug!(
        "Non-zero ranking scores: {:?}",
        scores
            .iter()
            .copied()
            .enumerate()
            .filter(|&(_, s)| s > 0.)
            .collect::<Vec<(usize, RankScore)>>()
    );
    scores
}

/// Rank nodes and sort them by "highest rank score first"
pub fn sort_by_rank(mut nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    let scores = rank_nodes(&nodes, fbas);

    nodes.sort_by(|x, y| scores[*y].partial_cmp(&scores[*x]).unwrap());
    nodes
}
