use actix_web::cookie::time::format_description;
use actix_web::{web, HttpResponse, Responder};
use datafusion::arrow::ipc::RecordBatch;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::json;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs;

#[derive(serde::Deserialize)]
pub struct InputData {
    data: HashMap<String, Value>,
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
    // register parquet file with the execution context
    ctx.register_parquet(
        "ph",
        &format!("pension_history.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .expect("Failed to register parquet file for processing");

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

    let mut result = ResultData::default();

    for j in 0..49 {
        let query = format!("SELECT sum(ph.amount{}) as calc{} FROM ph", j, j);
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
            result.add_result(calc_id.clone(), json!(value));
        }
    }

    for j in 1..49 {
        let query = format!("SELECT sum(ph.number{}) as calc{} FROM ph", j, j + 50);
        let df = ctx.sql(&query).await.expect("Failed to execute sql query");
        let calc_id = format!("calc{}", j + 50);
        // print the results
        let rec_batches = df.collect().await.unwrap();
        for rec in rec_batches {
            let calc_array = rec.column(0);
            let value = calc_array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);
            result.add_result(calc_id.clone(), json!(value));
        }
    }
    let content_type = "application/json".to_string();
    HttpResponse::Ok().content_type(content_type).json(&result)
}
