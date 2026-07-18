use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
use rdf_fusion_physical::distinct::SortedDistinctExec;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn run_distinct(batch: RecordBatch) {
    let schema = batch.schema();
    let ctx = SessionContext::new();

    let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap();
    let memory_exec = table.scan(&ctx.state(), None, &[], None).await.unwrap();

    let distinct_exec = SortedDistinctExec::new(
        memory_exec,
        LexRequirement::new(vec![PhysicalSortRequirement {
            expr: datafusion::physical_expr::expressions::col("val", &schema).unwrap(),
            options: None,
        }])
        .unwrap(),
    );

    let mut stream = distinct_exec.execute(0, ctx.task_ctx()).unwrap();

    while let Some(result) = stream.next().await {
        let _ = result.unwrap();
    }
}

fn bench_distinct(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("SortedDistinctStream_10k");

    let sizes = [0, 50, 100]; // Duplicates percentage

    for &dups in &sizes {
        group.bench_with_input(
            BenchmarkId::new("ObjectId", format!("{dups}%_dups")),
            &dups,
            |b, &dups| {
                let batch = generate_object_id_batch(10_000, dups);
                b.to_async(&rt).iter(|| run_distinct(batch.clone()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("String", format!("{dups}%_dups")),
            &dups,
            |b, &dups| {
                let batch = generate_string_batch(10_000, dups);
                b.to_async(&rt).iter(|| run_distinct(batch.clone()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("PlainTerm", format!("{dups}%_dups")),
            &dups,
            |b, &dups| {
                let batch = generate_plain_term_batch(10_000, dups);
                b.to_async(&rt).iter(|| run_distinct(batch.clone()));
            },
        );
    }

    group.finish();
}

fn generate_object_id_batch(num_rows: usize, dups_percent: u8) -> RecordBatch {
    let mut builder = Int64Array::builder(num_rows);
    let mut current_id = 0;

    for _ in 0..num_rows {
        builder.append_value(current_id);
        if rand::random::<u8>() % 100 >= dups_percent {
            current_id += 1;
        }
    }

    let array = Arc::new(builder.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn generate_string_batch(num_rows: usize, dups_percent: u8) -> RecordBatch {
    let mut builder = datafusion::arrow::array::StringBuilder::new();
    let mut current_id = 0;

    for _ in 0..num_rows {
        builder.append_value(format!("string_value_{current_id}"));
        if rand::random::<u8>() % 100 >= dups_percent {
            current_id += 1;
        }
    }

    let array = Arc::new(builder.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn generate_plain_term_batch(num_rows: usize, dups_percent: u8) -> RecordBatch {
    let mut builder = PlainTermArrayElementBuilder::with_capacity(num_rows);
    let mut current_id = 0;

    for _ in 0..num_rows {
        builder.append_raw(1, &format!("value_{current_id}"), None, None);
        if rand::random::<u8>() % 100 >= dups_percent {
            current_id += 1;
        }
    }

    let array = Arc::new(builder.finish().into_array_ref()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        array.data_type().clone(),
        false,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

criterion_group!(benches, bench_distinct);
criterion_main!(benches);
