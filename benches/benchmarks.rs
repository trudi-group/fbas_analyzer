#[macro_use]
extern crate criterion;
extern crate fbas_analyzer;

use criterion::black_box;
use criterion::Criterion;

use std::path::Path;

use fbas_analyzer::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
    // let orgs = Organizations::from_json_file(
    //     Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
    //     &fbas,
    // );

    c.bench_function("find_minimal_quorums", |b| {
        b.iter(|| find_minimal_quorums(black_box(&fbas)))
    });
    let minimal_quorums = find_minimal_quorums(&fbas);

    // c.bench_function("all_intersect", |b| {
    //     b.iter(|| all_intersect(black_box(&minimal_quorums)))
    // });

    let broken_fbas = Fbas::from_json_file(Path::new(
        "test_data/stellarbeat_nodes_2020-01-16_broken_by_hand.json",
    ));
    c.bench_function("find_nonintersecting_quorums_in_broken", |b| {
        b.iter(|| find_nonintersecting_quorums(black_box(&broken_fbas)))
    });
    // c.bench_function("find_nonintersecting_quorums_in_correct", |b| {
    //     b.iter(|| find_nonintersecting_quorums(black_box(&fbas)))
    // });

    // c.bench_function("collapse_by_organization", |b| {
    //     b.iter(|| {
    //         remove_non_minimal_node_sets(
    //             orgs.collapse_node_sets(black_box(minimal_quorums.clone())),
    //         )
    //     })
    // });
    // let minimal_quorums_collapsed =
    //     remove_non_minimal_node_sets(orgs.collapse_node_sets(minimal_quorums.clone()));

    // c.bench_function("find_minimal_blocking_sets_collapsed", |b| {
    //     b.iter(|| find_minimal_blocking_sets(black_box(&minimal_quorums_collapsed)))
    // });
    // c.bench_function("find_minimal_intersections_collapsed", |b| {
    //     b.iter(|| find_minimal_intersections(black_box(&minimal_quorums_collapsed)))
    // });

    c.bench_function("find_minimal_blocking_sets", |b| {
        b.iter(|| find_minimal_blocking_sets(black_box(&minimal_quorums)))
    });
    c.bench_function("find_minimal_intersections", |b| {
        b.iter(|| find_minimal_intersections(black_box(&minimal_quorums)))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
