use aev::create_table::create_memtable;
use clap::Parser;
use datafusion::{error::DataFusionError, prelude::SessionContext};
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = String::from("C:\\Windows\\System32\\winevt\\Logs\\Security.evtx") )]
    file: String,
    #[arg(long)]
    folder: Option<String>,
    #[arg(long)]
    table_name: String,
    #[arg(long)]
    query: String,
}

/// This example demonstrates executing a simple query against a Memtable
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = Args::parse();
    let file = args.file;
    let table_name = args.table_name.as_str();
    let query = args.query;

    //println!("Query: {}", query);

    let mem_table = create_memtable(file)?;

    // create local execution context
    let ctx = SessionContext::new();

    // Register the in-memory table containing the data
    ctx.register_table(table_name, Arc::new(mem_table))?;

    let dataframe = ctx
        .sql(&query) // "SELECT id, new_process_name, parent_process_name FROM records WHERE \"id\"=4688;"
        .await?;

    dataframe.show().await?;

    Ok(())
}
