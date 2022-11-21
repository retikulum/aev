use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use evtx::EvtxParser;
use std::sync::Arc;

pub fn create_memtable(filename: String) -> Result<MemTable> {
    MemTable::try_new(get_schema(), vec![vec![create_record_batch(filename)?]])
}

fn create_record_batch(filename: String) -> Result<RecordBatch> {
    // create intermediate vectors for parsing fields
    let mut parser = EvtxParser::from_path(filename).unwrap();
    let mut access_mask_vec: Vec<String> = Vec::new();
    let mut id_vec: Vec<u64> = Vec::new();
    let mut event_id_vec: Vec<u64> = Vec::new();
    let mut procname_vec: Vec<String> = Vec::new();
    let mut subject_user_name_vec: Vec<String> = Vec::new();
    let mut new_process_name_vec: Vec<String> = Vec::new();
    let mut parent_process_name_vec: Vec<String> = Vec::new();

    for record in parser.records_json_value() {
        match record {
            Ok(r) => {
                // Parse fields of record
                id_vec.push(r.event_record_id);
                if let Some(val) = r.data["Event"]["System"]["EventID"].as_u64() {
                    event_id_vec.push(val);
                }
                procname_vec.push(r.data["Event"]["EventData"]["ProcessName"].to_string());
                subject_user_name_vec
                    .push(r.data["Event"]["EventData"]["SubjectUserName"].to_string());
                access_mask_vec.push(r.data["Event"]["EventData"]["AccessMask"].to_string());
                new_process_name_vec
                    .push(r.data["Event"]["EventData"]["NewProcessName"].to_string());
                parent_process_name_vec
                    .push(r.data["Event"]["EventData"]["ParentProcessName"].to_string());
                //println!("{}", r.data);
            }
            Err(e) => eprintln!("{}", e),
        }
    }

    let access_mask_array = StringArray::from(access_mask_vec);
    let id_array = UInt64Array::from(event_id_vec);
    let procname_array = StringArray::from(procname_vec);
    let subjectusername_array = StringArray::from(subject_user_name_vec);
    let new_procname_array = StringArray::from(new_process_name_vec);
    let parent_procname_array = StringArray::from(parent_process_name_vec);

    Ok(RecordBatch::try_new(
        get_schema(),
        vec![
            Arc::new(id_array),
            Arc::new(procname_array),
            Arc::new(subjectusername_array),
            Arc::new(access_mask_array),
            Arc::new(new_procname_array),
            Arc::new(parent_procname_array),
        ],
    )?)
}

fn get_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("process_name", DataType::Utf8, true),
        Field::new("subject_user_name", DataType::Utf8, true),
        Field::new("access_mask_name", DataType::Utf8, false),
        Field::new("new_process_name", DataType::Utf8, false),
        Field::new("parent_process_name", DataType::Utf8, false),
    ]))
}
