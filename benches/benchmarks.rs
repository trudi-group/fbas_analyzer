#[macro_use]
extern crate criterion;
extern crate fbas_analyzer;

use criterion::black_box;
use criterion::Criterion;

use fbas_analyzer::{
    all_node_sets_interesect, get_minimal_blocking_sets, get_minimal_quorums, Fbas,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    let fbas = Fbas::from_json_file("test_data/stellarbeat_2019-08-02.json");
    let minimal_quorums = get_minimal_quorums(&fbas);

    c.bench_function("get_minimal_quorums", |b| {
        b.iter(|| get_minimal_quorums(black_box(&fbas)))
    });
    c.bench_function("all_node_sets_interesect", |b| {
        b.iter(|| all_node_sets_interesect(black_box(&minimal_quorums)))
    });
    c.bench_function("get_minimal_blocking_sets", |b| {
        b.iter(|| get_minimal_blocking_sets(black_box(&minimal_quorums)))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
