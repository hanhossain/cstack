mod node;
mod pager;
mod repl;
mod serialization;
mod table;
mod vm;

use repl::{print_prompt, read_input};
use table::Table;
use vm::{
    do_meta_command, execute_statement, prepare_statement, ExecuteError, MetaCommandError,
    PrepareError,
};

fn main() {
    let filename = std::env::args()
        .skip(1)
        .next()
        .expect("Must supply a database filename");
    let mut table = Table::open(&filename);

    loop {
        print_prompt();
        let input = read_input();

        unsafe {
            if input.starts_with(".") {
                match do_meta_command(&input, &mut table) {
                    Ok(_) => continue,
                    Err(MetaCommandError::UnrecognizedCommand) => {
                        println!("Unrecognized command '{}'", input);
                        continue;
                    }
                }
            }

            let statement = match prepare_statement(&input) {
                Ok(s) => s,
                Err(error) => match error {
                    PrepareError::NegativeId => {
                        println!("ID must be positive.");
                        continue;
                    }
                    PrepareError::StringTooLong => {
                        println!("String is too long.");
                        continue;
                    }
                    PrepareError::SyntaxError => {
                        println!("Syntax error. Could not parse statement.");
                        continue;
                    }
                    PrepareError::UnrecognizedStatement => {
                        println!("Unrecognized keyword at start of '{}'.", input);
                        continue;
                    }
                },
            };

            match execute_statement(&statement, &mut table) {
                Ok(_) => {
                    println!("Executed.");
                }
                Err(ExecuteError::DuplicateKey) => {
                    println!("Error: Duplicate key.");
                }
            }
        }
    }
}
