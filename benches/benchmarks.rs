#[macro_use]
extern crate criterion;
extern crate fbas_analyzer;

use criterion::black_box;
use criterion::Criterion;

use std::path::Path;

use fbas_analyzer::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    // fbas with a symmetric top tier
    let fbas_stt = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));

    // fbas with a slightly asymmetric top tier
    let mut fbas = fbas_stt.clone();
    let mut quorum_set = fbas.get_quorum_set(1).unwrap();
    quorum_set.inner_quorum_sets[0].validators.pop();
    fbas.swap_quorum_set(1, quorum_set);

    c.bench_function("to_standard_form", |b| b.iter(|| fbas.to_standard_form()));

    let fbas = fbas.to_standard_form();
    let fbas_stt = fbas_stt.to_standard_form();

    c.bench_function("find_minimal_quorums", |b| {
        b.iter(|| find_minimal_quorums(black_box(&fbas)))
    });
    c.bench_function("find_minimal_quorums_symmetric_top_tier", |b| {
        b.iter(|| find_minimal_quorums(black_box(&fbas_stt)))
    });
    c.bench_function("find_minimal_blocking_sets", |b| {
        b.iter(|| find_minimal_blocking_sets(black_box(&fbas)))
    });
    c.bench_function("find_minimal_blocking_sets_symmetric_top_tier", |b| {
        b.iter(|| find_minimal_blocking_sets(black_box(&fbas_stt)))
    });

    let minimal_quorums = find_minimal_quorums(&fbas);
    let minimal_quorums_stt = find_minimal_quorums(&fbas_stt);
    c.bench_function("find_minimal_splitting_sets", |b| {
        b.iter(|| find_minimal_splitting_sets(black_box(&minimal_quorums)))
    });
    c.bench_function("find_minimal_splitting_sets_symmetric_top_tier", |b| {
        b.iter(|| find_minimal_splitting_sets(black_box(&minimal_quorums_stt)))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
