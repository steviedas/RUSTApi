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
        Field::new("Depot", DataType::Utf8, false),
        Field::new("R2_Depot", DataType::Utf8, false),
        Field::new("TOC", DataType::Utf8, false),
        Field::new("Railway_Period", DataType::Utf8, false),
        // Field::new("Date_Reported", DataType::Date32, false),
        // Field::new("Date_Completed", DataType::Date32, false),
        // Field::new("Date", DataType::Date32, false),
        Field::new("Fleet_Name", DataType::Utf8, false),
        Field::new("Vehicle_Class", DataType::Utf8, false),
        Field::new("Vehicle_Sub_Class", DataType::Utf8, false),
        Field::new("Unit", DataType::Utf8, false),
        Field::new("Vehicle", DataType::Utf8, false),
        Field::new("Intervention_ID", DataType::Utf8, false),
        Field::new("Intervention_Type", DataType::Utf8, false),
        Field::new("Intervention_Key", DataType::Utf8, false),
        Field::new("Component_Mileage", DataType::Float32, false),
        // Field::new("Is_TIN", DataType::Bool, false),
        Field::new("Intervention_Report", DataType::Utf8, false),
        Field::new("Intervention_Action", DataType::Utf8, false),
        Field::new("Work_Order_Type", DataType::Utf8, false),
        Field::new("Work_Order_Equipment", DataType::Utf8, false),
        Field::new("Work_Order_Status", DataType::Utf8, false),
        Field::new("System_Primary_Code", DataType::Utf8, false),
        Field::new("System_Primary_Code_Desc", DataType::Utf8, false),
        Field::new("System_Secondary_Code", DataType::Utf8, false),
        Field::new("System_Secondary_Code_Desc", DataType::Utf8, false),
        Field::new("Priority", DataType::Utf8, false),
        Field::new("Warranty", DataType::Utf8, false),
        Field::new("Location", DataType::Utf8, false),
        Field::new("Sum_Prim_Delay_Mins", DataType::Int8, false),
        Field::new("Sum_Reactnry_Delay_Mins", DataType::Int8, false),
        Field::new("Sum_Total_Delay_Mins", DataType::Int8, false),
        Field::new("Count_Full_Cancellations", DataType::Int8, false),
        Field::new("Count_Part_Cancellations", DataType::Int8, false),
        Field::new("Count_Total_Cancellations", DataType::Float32, false),
        // Field::new("Porterbrook_Asset", DataType::Bool, false),                
        // Field::new("Is_Autocoded", DataType::Bool, false),
        // Field::new("Date_Autocoded", DataType::Date32, false),
        Field::new("P_Symptom_Inferred", DataType::Utf8, false),
        Field::new("Root_Code_Secondary_Probability", DataType::Float32, false),
        Field::new("Root_Code_Tertiary_Probability", DataType::Float32, false),
        Field::new("Symptom_Code_Probability", DataType::Float32, false),
        Field::new("P_Symptom_Verified", DataType::Utf8, false),
        Field::new("P_Root_Code_Inferred_Secondary", DataType::Utf8, false),
        Field::new("P_Root_Code_Inferred_Secondary_Desc", DataType::Utf8, false),
        Field::new("P_Root_Code_Inferred_Tertiary", DataType::Utf8, false),
        Field::new("P_Root_Code_Inferred_Tertiary_Desc", DataType::Utf8, false),
        Field::new("P_Symptom_Code_Inferred", DataType::Utf8, false),
        Field::new("P_Symptom_Code_Inferred_Desc", DataType::Utf8, false),
        Field::new("P_Root_Code_Verified", DataType::Utf8, false),
        Field::new("P_Root_Code_Verified_Desc", DataType::Utf8, false),
        // Field::new("Is_Verified", DataType::Bool, false),
        // Field::new("Date_Verified", DataType::Date32, false),
        Field::new("Master_Intervention_Key", DataType::Utf8, false),
        // Field::new("Is_Relevant", DataType::Bool, false),
        // Field::new("Is_Power_Pack_Intervention", DataType::Bool, false),
        Field::new("Model_URI_Root_Code_Secondary", DataType::Utf8, false),
        Field::new("Model_URI_Root_Code_Tertiary", DataType::Utf8, false),
        Field::new("Model_URI_Symptom", DataType::Utf8, false),
        Field::new("_Data_Source", DataType::Utf8, false),
        // Field::new("_Time_Added", DataType::Timestamp, false),
        // Field::new("_Ingest_Timestamp", DataType::Timestamp, false),
        Field::new("_UID", DataType::Utf8, false),
    ]));

    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let R2_Depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let TOC_values = StringArray::from(vec!["NEW MALDEN"]);
    let Railway_Period_values = StringArray::from(vec!["NEW MALDEN"]);
    let Fleet_Name_values = StringArray::from(vec!["NEW MALDEN"]);
    let Vehicle_Class_values = StringArray::from(vec!["NEW MALDEN"]);
    let Vehicle_Sub_Class_values = StringArray::from(vec!["NEW MALDEN"]);
    let Unit_values = StringArray::from(vec!["NEW MALDEN"]);
    let Vehicle_values = StringArray::from(vec!["NEW MALDEN"]);
    let Intervention_ID_values = StringArray::from(vec!["NEW MALDEN"]);
    let Intervention_Type_values = StringArray::from(vec!["NEW MALDEN"]);
    let Intervention_Key_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);
    let depot_values = StringArray::from(vec!["NEW MALDEN"]);


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