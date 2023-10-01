use std::collections::HashMap;
use std::sync::Arc;
use deltalake::{builder::DeltaTableBuilder, DeltaTable};
use deltalake::operations::DeltaOps;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

fn get_data_to_write() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids : Vec<i32> = (1..=10).map(i32::from).collect();
    let names : Vec<String> = ids.iter().map(|x| format!("item {x}")).collect();

    let id_values = Int32Array::from(ids);
    let name_values = StringArray::from(names);

    RecordBatch::try_new(schema, vec![Arc::new(id_values), Arc::new(name_values)]).unwrap()
}

async fn append_to_table(path: String, access_key: String,batch: RecordBatch) -> DeltaTable {

    let mut backend_config: HashMap<String, String> = HashMap::new();
    backend_config.insert("azure_storage_access_key".to_string(), access_key);

    let table = DeltaTableBuilder::from_uri(path)
        .with_storage_options(backend_config)
        .build()
        .unwrap();

    let ops = DeltaOps::from(table);

    let commit_result = ops.write(vec![batch.clone()]).await.unwrap();
    commit_result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let azure_storage_location = std::env::var("azure_storage_location").unwrap();
    let azure_storage_access_key = std::env::var("azure_storage_access_key").unwrap();
    let batch = get_data_to_write();
    let table = append_to_table(azure_storage_location, azure_storage_access_key, batch).await;
    println!("Data inserted with version : {}", table.version());
}