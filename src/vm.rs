use crate::node::{leaf_node_insert, LeafNode};
use crate::repl::{print_constants, print_tree};
use crate::serialization::{deserialize_row, Row, COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
use crate::table::Table;
use libc::{strcpy, EXIT_SUCCESS};
use std::ffi::CString;
use std::process::exit;
use std::str::FromStr;

pub enum Statement {
    Insert(Row),
    Select,
}

pub enum PrepareError {
    NegativeId,
    StringTooLong,
    SyntaxError,
    UnrecognizedStatement,
}

pub unsafe fn prepare_statement(input: &str) -> Result<Statement, PrepareError> {
    if &input[..6] == "insert" {
        prepare_insert(input)
    } else if input == "select" {
        Ok(Statement::Select)
    } else {
        Err(PrepareError::UnrecognizedStatement)
    }
}

#[allow(temporary_cstring_as_ptr)]
unsafe fn prepare_insert(input: &str) -> Result<Statement, PrepareError> {
    let mut splitter = input.split(" ");
    let _keyword = splitter.next();
    let id_string = splitter.next();
    let username = splitter.next();
    let email = splitter.next();

    let (id_string, username, email) = match (id_string, username, email) {
        (Some(x), Some(y), Some(z)) => (x, y, z),
        _ => return Err(PrepareError::SyntaxError),
    };

    let id = i32::from_str(id_string).unwrap();
    if id < 0 {
        return Err(PrepareError::NegativeId);
    }

    if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
        return Err(PrepareError::StringTooLong);
    }

    let mut row = Row::new();

    row.id = id as u32;
    strcpy(
        row.username.as_mut_ptr(),
        CString::new(username).unwrap().as_ptr(),
    );
    strcpy(
        row.email.as_mut_ptr(),
        CString::new(email).unwrap().as_ptr(),
    );

    Ok(Statement::Insert(row))
}

pub enum MetaCommandError {
    UnrecognizedCommand,
}

pub unsafe fn do_meta_command(query: &str, table: &mut Table) -> Result<(), MetaCommandError> {
    match query {
        ".exit" => {
            table.close();
            exit(EXIT_SUCCESS);
        }
        ".btree" => {
            println!("Tree:");
            print_tree(&mut table.pager, 0, 0);
            Ok(())
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
            Ok(())
        }
        _ => Err(MetaCommandError::UnrecognizedCommand),
    }
}

pub enum ExecuteError {
    DuplicateKey,
}

unsafe fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecuteError> {
    let node = table.pager.get_page(table.root_page_num as usize);
    let leaf_node = LeafNode::new(node.buffer);
    let num_cells = leaf_node.num_cells();

    let key_to_insert = row.id;
    let mut cursor = table.find(key_to_insert);

    if cursor.cell_num < num_cells {
        let key_at_index = leaf_node.key(cursor.cell_num);
        if key_at_index == key_to_insert {
            return Err(ExecuteError::DuplicateKey);
        }
    }

    leaf_node_insert(&mut cursor, row.id, row);
    Ok(())
}

unsafe fn execute_select(_statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        let mut row = Row::new();
        deserialize_row(cursor.value(), &mut row);
        row.print_row();
        cursor.advance();
    }

    Ok(())
}

pub unsafe fn execute_statement(
    statement: &Statement,
    table: &mut Table,
) -> Result<(), ExecuteError> {
    match statement {
        Statement::Insert(row) => execute_insert(row, table),
        Statement::Select => execute_select(statement, table),
    }
}
