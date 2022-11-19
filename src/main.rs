use evtx::EvtxParser;
use clap::Parser;

#[derive(Parser, Debug)]
struct Args{
    #[arg(long, default_value_t = String::from("C:/Windows/System32/winevt/Logs/Security.evtx") )]
    file: String,
    #[arg(long)]
    folder: Option<String>,
}


fn main() {
    
    let args = Args::parse();

    let file = args.file;
    let mut parser = EvtxParser::from_path(file).unwrap();
    for record in parser.records() {
        match record {
            Ok(r) => println!("Record {}\n{}", r.event_record_id, r.data),
            Err(e) => eprintln!("{}", e),
        }
    }
}
