use axum_test::TestServer;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use rdf_fusion::model::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use rdf_fusion::store::Store;
use rdf_fusion_web::{AppState, create_router};
use std::sync::Arc;
use tokio::runtime::Builder;

fn encode_solution(criterion: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let store = Store::new_with_datafusion_config(
        SessionConfig::new().with_target_partitions(1),
        RuntimeEnv::default().into(),
    );
    let quads = generate_quads(8192).collect::<Vec<_>>();
    runtime.block_on(async {
        store.extend(quads.iter().map(Quad::as_ref)).await.unwrap();
    });

    let app_state = AppState {
        store: Arc::new(store),
        read_only: false,
        union_default_graph: false,
    };

    let app = create_router(app_state);
    let server = TestServer::new(app).unwrap();

    criterion.bench_function("Web: Encode SELECT Result", |b| {
        b.to_async(&runtime).iter(async || {
            let query = "SELECT * WHERE { ?s ?p ?o }";
            let response = server
                .get("/repositories/default/query")
                .add_query_param("query", query)
                .expect_success()
                .await;
            assert_eq!(response.text().len(), 1495862);
        })
    });
}

fn generate_quads(count: usize) -> impl Iterator<Item = Quad> {
    (0..count).map(|i| {
        let subject = format!("http://example.com/subject{}", i);
        let predicate = format!("http://example.com/predicate{}", i);
        let object = format!("http://example.com/object{}", i);
        Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(subject)),
            NamedNode::new_unchecked(predicate),
            Term::NamedNode(NamedNode::new_unchecked(object)),
            GraphName::DefaultGraph,
        )
    })
}

criterion_group!(encode_results, encode_solution);
criterion_main!(encode_results);
