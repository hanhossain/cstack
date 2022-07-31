use crate::node::{leaf_node_insert, leaf_node_key, leaf_node_num_cells};
use crate::pager::get_page;
use crate::repl::{print_constants, print_tree};
use crate::serialization::{
    deserialize_row, print_row, Row, COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE,
};
use crate::table::Table;
use libc::{exit, strcpy, EXIT_SUCCESS};
use std::ffi::CString;
use std::str::FromStr;

pub enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    pub r#type: StatementType,
    pub row_to_insert: Row, // only used by insert statement
}

pub enum PrepareError {
    NegativeId,
    StringTooLong,
    SyntaxError,
    UnrecognizedStatement,
}

pub unsafe fn prepare_statement(
    input: &mut String,
    statement: &mut Statement,
) -> Result<(), PrepareError> {
    if &input[..6] == "insert" {
        prepare_insert(input, statement)
    } else if input == "select" {
        statement.r#type = StatementType::Select;
        Ok(())
    } else {
        Err(PrepareError::UnrecognizedStatement)
    }
}

#[allow(temporary_cstring_as_ptr)]
unsafe fn prepare_insert(
    input: &mut String,
    statement: &mut Statement,
) -> Result<(), PrepareError> {
    statement.r#type = StatementType::Insert;
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

    statement.row_to_insert.id = id as u32;
    strcpy(
        statement.row_to_insert.username.as_mut_ptr(),
        CString::new(username).unwrap().as_ptr(),
    );
    strcpy(
        statement.row_to_insert.email.as_mut_ptr(),
        CString::new(email).unwrap().as_ptr(),
    );

    Ok(())
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

unsafe fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    let node = get_page(&mut table.pager, table.root_page_num as usize);
    let num_cells = *leaf_node_num_cells(node);

    let row_to_insert = &statement.row_to_insert;
    let key_to_insert = row_to_insert.id;
    let mut cursor = table.find(key_to_insert);

    if cursor.cell_num < num_cells {
        let key_at_index = *leaf_node_key(node, cursor.cell_num);
        if key_at_index == key_to_insert {
            return Err(ExecuteError::DuplicateKey);
        }
    }

    leaf_node_insert(&mut cursor, row_to_insert.id, row_to_insert);
    Ok(())
}

unsafe fn execute_select(_statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        let mut row = Row::new();
        deserialize_row(cursor.value(), &mut row);
        print_row(&row);
        cursor.advance();
    }

    Ok(())
}

pub unsafe fn execute_statement(
    statement: &Statement,
    table: &mut Table,
) -> Result<(), ExecuteError> {
    match statement.r#type {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => execute_select(statement, table),
    }
}
