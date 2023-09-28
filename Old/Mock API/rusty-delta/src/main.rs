use std::collections::HashMap;
use std::sync::Arc;
use deltalake::{builder::DeltaTableBuilder, DeltaTable};
use deltalake::action;
use deltalake::action::{DeltaOperation, SaveMode};
use deltalake::writer::DeltaWriter;
use deltalake::writer::RecordBatchWriter;
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

async fn commit_transaction(table: &mut DeltaTable, batch: RecordBatch) -> i64 {
    table.load().await.unwrap();
    let mut writer = RecordBatchWriter::for_table(table).unwrap();
    writer.write(batch).await.unwrap();

    let actions = writer
        .flush()
        .await
        .unwrap()
        .iter()
        .map(|add| {
            let clone = add.clone();
            action::Action::add(clone.into())
        })
        .collect();

    let mut transaction = table.create_transaction(None);

    transaction.add_actions(actions);

    let commit_result = transaction
        .commit(
            Some(DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            }),
            None,
        )
        .await
        .unwrap();
    commit_result
}

async fn append_to_table(path: String, access_key: String,batch: RecordBatch) -> DeltaTable {

    let mut backend_config: HashMap<String, String> = HashMap::new();
    backend_config.insert("azure_storage_access_key".to_string(), access_key);

    let mut table = DeltaTableBuilder::from_uri(path)
        .with_storage_options(backend_config)
        .build()
        .unwrap();

        commit_transaction(&mut table, batch).await;
        table
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let azure_storage_location = std::env::var("azure_storage_location").unwrap();
    let azure_storage_access_key = std::env::var("azure_storage_access_key").unwrap();
    let batch = get_data_to_write();
    let table = append_to_table(azure_storage_location, azure_storage_access_key, batch).await;
    println!("Data inserted with version : {}", table.version());
}