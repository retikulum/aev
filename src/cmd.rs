use crate::create_table::create_memtable;
use datafusion::prelude::SessionContext;
use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};
use std::sync::Arc;

/// function for interactive mode
/// In this mode, "file", "table_name", "query" keywords are allowed for now
/// It simply use rustyline to manipulate terminal
pub async fn interactive_mode() -> Result<()> {
    // create datafusion session
    let ctx = SessionContext::new();
    let mut filename = String::from("");
    // rustyline editor
    let mut rl = Editor::<()>::new()?;

    if rl.load_history("aev_history.txt").is_err() {
        eprintln!("No prev history");
    }
    loop {
        // aev> is used as starting keyword for interactive mode
        let readline = rl.readline("aev>");
        match readline {
            Ok(line) => {
                // find index of space to split line
                // then split it into two string
                if let Some(space_index) = line.find(" ") {
                    let line_as_seperated = line.split_at(space_index + 1);
                    let first_word = line_as_seperated.0.to_lowercase();
                    let second_word = line_as_seperated.1.to_string();
                    //println!("second word: {}", second_word);
                    if first_word == "file " {
                        filename = second_word;
                    } else if first_word == "table_name " {
                        // check filename. it shouldnt be the same as default
                        if filename.ne("") {
                            //println!("{}", filename);
                            //create table with filename and table name
                            if let Ok(table) = create_memtable(filename.to_owned()) {
                                ctx.register_table(second_word.as_str(), Arc::new(table))
                                    .unwrap();
                            }
                        }
                    } else if first_word == "query " {
                        // run query
                        let dataframe = ctx
                            .sql(&second_word) // "SELECT id, new_process_name, parent_process_name FROM records WHERE \"id\"=4688;"
                            .await
                            .unwrap();
                        dataframe.show().await.unwrap();
                    }
                } else {
                    eprintln!("Unknown command!");
                    continue;
                }
                rl.add_history_entry(line.as_str());
                println!("{}", line);
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("aev_history.txt")
}
