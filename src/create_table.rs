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
    let mut handle_id_vec: Vec<String> = Vec::new();
    let mut object_name_vec: Vec<String> = Vec::new();
    let mut object_server_vec: Vec<String> = Vec::new();
    let mut object_type_vec: Vec<String> = Vec::new();
    let mut privilige_list_vec: Vec<String> = Vec::new();
    let mut proc_id_vec: Vec<String> = Vec::new();
    let mut subject_domain_name_vec: Vec<String> = Vec::new();
    let mut subject_logon_id_vec: Vec<String> = Vec::new();
    let mut subject_user_sid_vec: Vec<String> = Vec::new();
    let mut channel_vec: Vec<String> = Vec::new();
    let mut computer_vec: Vec<String> = Vec::new();

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
                handle_id_vec
                    .push(r.data["Event"]["EventData"]["HandleId"].to_string());
                object_name_vec
                    .push(r.data["Event"]["EventData"]["ObjectName"].to_string());
                object_server_vec
                    .push(r.data["Event"]["EventData"]["ObjectServer"].to_string());
                object_type_vec
                    .push(r.data["Event"]["EventData"]["ObjectType"].to_string());
                privilige_list_vec
                    .push(r.data["Event"]["EventData"]["PriviligeList"].to_string());
                proc_id_vec
                    .push(r.data["Event"]["EventData"]["ProcessId"].to_string());
                subject_domain_name_vec
                    .push(r.data["Event"]["EventData"]["SubjectDomainName"].to_string());
                subject_logon_id_vec
                    .push(r.data["Event"]["EventData"]["SubjectLoginId"].to_string());
                subject_user_sid_vec
                    .push(r.data["Event"]["EventData"]["SubjectUserSid"].to_string());
                channel_vec
                    .push(r.data["Event"]["System"]["Channel"].to_string());
                computer_vec
                    .push(r.data["Event"]["System"]["Computer"].to_string());
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
    let handle_id_array = StringArray::from(handle_id_vec);
    let object_name_array = StringArray::from(object_name_vec);
    let object_server_array = StringArray::from(object_server_vec);
    let object_type_array = StringArray::from(object_type_vec);
    let privilige_list_array = StringArray::from(privilige_list_vec);
    let proc_id_array = StringArray::from(proc_id_vec);
    let subject_domain_name_array = StringArray::from(subject_domain_name_vec);
    let subject_logon_id_array = StringArray::from(subject_logon_id_vec);
    let subject_user_sid_array = StringArray::from(subject_user_sid_vec);
    let channel_array = StringArray::from(channel_vec);
    let computer_array = StringArray::from(computer_vec);

    Ok(RecordBatch::try_new(
        get_schema(),
        vec![
            Arc::new(id_array),
            Arc::new(procname_array),
            Arc::new(subjectusername_array),
            Arc::new(access_mask_array),
            Arc::new(new_procname_array),
            Arc::new(parent_procname_array),
            Arc::new(handle_id_array),
            Arc::new(object_name_array),
            Arc::new(object_server_array),
            Arc::new(object_type_array),
            Arc::new(privilige_list_array),
            Arc::new(proc_id_array),
            Arc::new(subject_domain_name_array),
            Arc::new(subject_logon_id_array),
            Arc::new(subject_user_sid_array),
            Arc::new(channel_array),
            Arc::new(computer_array),
        ],
    )?)
}

fn get_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("EventID", DataType::UInt64, false),
        Field::new("ProcessName", DataType::Utf8, true),
        Field::new("SubjectUserName", DataType::Utf8, true),
        Field::new("AccessMaskName", DataType::Utf8, false),
        Field::new("NewProcessName", DataType::Utf8, false),
        Field::new("ParentProcessName", DataType::Utf8, false),
        Field::new("HandleId", DataType::Utf8, false),
        Field::new("ObjectName", DataType::Utf8, false),
        Field::new("ObjectServer", DataType::Utf8, false),
        Field::new("ObjectType", DataType::Utf8, false),
        Field::new("PriviligeList", DataType::Utf8, false),
        Field::new("ProcessId", DataType::Utf8, false),
        Field::new("SubjectDomainName", DataType::Utf8, false),
        Field::new("SubjectLogonId", DataType::Utf8, false),
        Field::new("SubjectUserSid", DataType::Utf8, false),
        Field::new("Channel", DataType::Utf8, false),
        Field::new("Computer", DataType::Utf8, false),
    ]))
}
