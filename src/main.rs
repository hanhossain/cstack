use cstack::repl::{print_prompt, read_input, InputBuffer};
use cstack::serialization::Row;
use cstack::table::db_open;
use cstack::vm::{
    do_meta_command, execute_statement, prepare_statement, ExecuteResult, MetaCommandResult,
    PrepareResult, Statement, StatementType,
};
use std::ffi::{CStr, CString};

fn main() {
    let mut args = std::env::args();
    let _ = args.next();
    let filename = args.next().expect("Must supply a database filename");
    unsafe {
        let filename_owned = CString::new(filename).unwrap();
        let filename = filename_owned.as_ptr();
        let mut table = db_open(filename);

        let mut input_buffer = InputBuffer::new();

        loop {
            print_prompt();
            read_input(&mut input_buffer);

            let meta_owned = CString::new(".").unwrap();
            let meta = meta_owned.as_ptr();
            if *input_buffer.buffer == *meta {
                match do_meta_command(&*input_buffer, &mut table) {
                    MetaCommandResult::META_COMMAND_SUCCESS => continue,
                    MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND => {
                        println!(
                            "Unrecognized command '{}'",
                            CStr::from_ptr(input_buffer.buffer).to_str().unwrap()
                        );
                        continue;
                    }
                }
            }

            let mut statement = Statement {
                r#type: StatementType::STATEMENT_SELECT,
                row_to_insert: Row::new(),
            };

            match prepare_statement(&mut input_buffer, &mut statement) {
                PrepareResult::PREPARE_SUCCESS => (),
                PrepareResult::PREPARE_NEGATIVE_ID => {
                    println!("ID must be positive.");
                    continue;
                }
                PrepareResult::PREPARE_STRING_TOO_LONG => {
                    println!("String is too long.");
                    continue;
                }
                PrepareResult::PREPARE_SYNTAX_ERROR => {
                    println!("Syntax error. Could not parse statement.");
                    continue;
                }
                PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT => {
                    println!(
                        "Unrecognized keyword at start of '{}'.",
                        CStr::from_ptr(input_buffer.buffer).to_str().unwrap()
                    );
                    continue;
                }
            }

            match execute_statement(&statement, &mut table) {
                ExecuteResult::EXECUTE_SUCCESS => {
                    println!("Executed.");
                }
                ExecuteResult::EXECUTE_DUPLICATE_KEY => {
                    println!("Error: Duplicate key.");
                }
            }
        }
    }
}
