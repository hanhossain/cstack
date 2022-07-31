use cstack::repl::{print_prompt, read_input};
use cstack::serialization::Row;
use cstack::table::Table;
use cstack::vm::{
    do_meta_command, execute_statement, prepare_statement, ExecuteResult, MetaCommandResult,
    PrepareResult, Statement, StatementType,
};
use std::ffi::CString;

fn main() {
    let mut args = std::env::args();
    let _ = args.next();
    let filename = args.next().expect("Must supply a database filename");
    unsafe {
        let filename_owned = CString::new(filename).unwrap();
        let filename = filename_owned.as_ptr();
        let mut table = Table::open(filename);

        loop {
            print_prompt();
            let mut input = read_input();

            if input.starts_with(".") {
                match do_meta_command(&input, &mut table) {
                    MetaCommandResult::META_COMMAND_SUCCESS => continue,
                    MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND => {
                        println!("Unrecognized command '{}'", input);
                        continue;
                    }
                }
            }

            let mut statement = Statement {
                r#type: StatementType::STATEMENT_SELECT,
                row_to_insert: Row::new(),
            };

            match prepare_statement(&mut input, &mut statement) {
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
                    println!("Unrecognized keyword at start of '{}'.", input);
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
