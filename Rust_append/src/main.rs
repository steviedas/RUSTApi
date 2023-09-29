use deltalake::{DeltaTable};
use deltalake::operations::DeltaOps;
use datafusion_expr::{col, lit};
use datafusion_utils::Expression;

// fn get_data_to_write() -> RecordBatch {
//     let schema = Arc::new(ArrowSchema::new(vec![
//         Field::new("id", DataType::Int32, false),
//         Field::new("name", DataType::Utf8, false),
//     ]));

//     let ids : Vec<i32> = (1..=400000).map(i32::from).collect();
//     let names : Vec<String> = ids.iter().map(|x| format!("item {x}")).collect();

//     let id_values = Int32Array::from(ids);
//     let name_values = StringArray::from(names);

//     RecordBatch::try_new(schema, vec![Arc::new(id_values), Arc::new(name_values)]).unwrap()
// }

// async fn load_table(path: String, access_key: String,batch: RecordBatch) -> DeltaTable {

//     let mut backend_config: HashMap<String, String> = HashMap::new();
//     backend_config.insert("azure_storage_access_key".to_string(), access_key);

//     let table = DeltaTableBuilder::from_uri(path)
//         .with_storage_options(backend_config)
//         .build()
//         .unwrap();

//     let ops = DeltaOps::from(table);

//     let commit_result = ops.write(vec![batch.clone()]).await.unwrap();
//     commit_result
// }

async fn load_table(path: String) -> DeltaTable {
    let table = deltalake::open_table((path))
        .await
        .unwrap();
    return table;
}

async fn append_table(path: String, target_path: String) -> DeltaTable {
    let target_table = load_table(target_path).await;
    let source_table = load_table(path).await;
    let Ok((table, metrics)) = DeltaOps(target_table)
        .merge(source_table, col("Intervention_Key").eq(col("source_table.Intervention_Key")))
        .with_source_alias("source_table")
        .when_matched_update(|update| {
            update
                .update("Count_Full_Cancellations", col("source_table.Count_Full_Cancellations") + lit(1))
        })?;

    // println!({metrics}.to_string());
    return table;
}


#[tokio::main(flavor = "current_thread")]
async fn main() {
    let azure_storage_location = std::env::var("AZURE_STORAGE_LOCATION").unwrap();
    let azure_storage_access_key = std::env::var("AZURE_STORAGE_ACCESS_KEY").unwrap();
    let azure_storage_location_base = std::env::var("AZURE_STORAGE_LOCATION_BASE").unwrap();
    let table = append_table(azure_storage_location_base, azure_storage_location).await;
    println!("Data inserted with version : {}", table.version());
}