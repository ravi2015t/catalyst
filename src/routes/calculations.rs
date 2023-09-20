use actix_web::{web, HttpResponse, Responder};
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::UInt64Array;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
// use tokio::time::Instant;

#[derive(serde::Deserialize)]
pub struct InputData {
    data: HashMap<String, Value>,
    id: u16,
}

#[derive(serde::Serialize)]
pub struct ResultData {
    results: HashMap<String, Value>,
}
impl ResultData {
    pub fn default() -> ResultData {
        ResultData {
            results: HashMap::new(),
        }
    }

    pub fn add_result(&mut self, key: String, value: Value) {
        self.results.insert(key, value);
    }
}
pub async fn caculate(input: web::Json<InputData>) -> impl Responder {
    let ctx = SessionContext::new();
    let start = Instant::now();

    // register parquet file with the execution context
    let table_path = format!("pensionHistory/{}/file.parquet", input.id);
    ctx.register_parquet("ph", &table_path, ParquetReadOptions::default())
        .await
        .expect("Failed to register parquet file for processing");
    let load_all_data_query = "SELECT * from ph";
    let all_data = ctx
        .sql(load_all_data_query)
        .await
        .expect("Loading all data failed");
    let all_data = all_data.collect().await.unwrap();

    let end = Instant::now();
    log::info!(
        "Time to load all data into memory {:?}  for id {}",
        end - start,
        input.id
    );

    // Create a DataFusion data source from the loaded RecordBatches
    let schema = all_data[0].schema(); // Assuming all batches have the same schema

    let table = MemTable::try_new(schema, vec![all_data]).expect("Creating memtable failed");
    ctx.register_table("pension_history", Arc::new(table))
        .expect("Registering memtable failed");
    //Aggregated query
    // let mut query = String::new();
    // query.push_str("SELECT ");
    // for i in 1..48 {
    //     let q = format!("sum(ph.amount{}) as calc{}, ", i, i);
    //     query.push_str(&q);
    // }
    // query.push_str("sum(ph.amount49) as calc49 ");
    // query.push_str("FROM ph");
    // let df = ctx.sql(&query).await.expect("Failed to execute sql query");
    // // print the results
    // let result = df.collect().await.unwrap();
    // for rec in result {
    //     serde_json::to_value(rec).unwrap();
    //     writer
    //         .write(&rec)
    //         .expect("Failed to write record batches to json");
    // }

    //Separate 100 calcs
    // execute the query

    let end = Instant::now();
    log::info!(
        "Time to register all data into mem table{:?}  for id {}",
        end - start,
        input.id
    );

    let result = ResultData::default();
    let mut query_tasks = Vec::new();

    let result = Arc::new(Mutex::new(result));
    let ctx = Arc::new(ctx);

    for j in 0..49 {
        let res = Arc::clone(&result);
        let ctx = ctx.clone();

        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum(pension_history.amount{}) as calc{} FROM pension_history",
                j, j
            );
            let df = ctx.sql(&query).await.expect("Failed to execute sql query");
            let calc_id = format!("calc{}", j);
            // print the results
            let rec_batches = df.collect().await.unwrap();
            for rec in rec_batches {
                let calc_array = rec.column(0);
                let value = calc_array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                let mut result = res.lock().unwrap();
                result.add_result(calc_id.clone(), json!(value));
            }
        });
        query_tasks.push(task);
    }

    for j in 0..49 {
        let res = Arc::clone(&result);
        let ctx = ctx.clone();

        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum(pension_history.number{}) as calc{} FROM pension_history",
                j, j
            );
            let df = ctx.sql(&query).await.expect("Failed to execute sql query");
            let calc_id = format!("calc{}", j);
            // print the results
            let rec_batches = df.collect().await.unwrap();
            for rec in rec_batches {
                let calc_array = rec.column(0);
                let value = calc_array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0);
                let mut result = res.lock().unwrap();
                result.add_result(calc_id.clone(), json!(value));
            }
        });
        query_tasks.push(task);
    }

    for task in query_tasks {
        task.await
            .expect("Waiting for all tasks to complete failed");
    }

    let end = Instant::now();
    log::info!(
        "Time taken to compute for id : {} - {:?}",
        input.id,
        end - start
    );
    let result = result.lock().unwrap();
    let json_response = serde_json::to_string(&*result).unwrap();
    let content_type = "application/json".to_string();
    HttpResponse::Ok()
        .content_type(content_type)
        .json(&json_response)
}
