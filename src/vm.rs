#![allow(non_camel_case_types)]

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
    STATEMENT_INSERT,
    STATEMENT_SELECT,
}

pub struct Statement {
    pub r#type: StatementType,
    pub row_to_insert: Row, // only used by insert statement
}

pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
}

pub unsafe fn prepare_statement(input: &mut String, statement: &mut Statement) -> PrepareResult {
    if &input[..6] == "insert" {
        prepare_insert(input, statement)
    } else if input == "select" {
        statement.r#type = StatementType::STATEMENT_SELECT;
        PrepareResult::PREPARE_SUCCESS
    } else {
        PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT
    }
}

#[allow(temporary_cstring_as_ptr)]
unsafe fn prepare_insert(input: &mut String, statement: &mut Statement) -> PrepareResult {
    statement.r#type = StatementType::STATEMENT_INSERT;
    let mut splitter = input.split(" ");
    let _keyword = splitter.next();
    let id_string = splitter.next();
    let username = splitter.next();
    let email = splitter.next();

    let (id_string, username, email) = match (id_string, username, email) {
        (Some(x), Some(y), Some(z)) => (x, y, z),
        _ => return PrepareResult::PREPARE_SYNTAX_ERROR,
    };

    let id = i32::from_str(id_string).unwrap();
    if id < 0 {
        return PrepareResult::PREPARE_NEGATIVE_ID;
    }

    if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
        return PrepareResult::PREPARE_STRING_TOO_LONG;
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

    PrepareResult::PREPARE_SUCCESS
}

pub enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
}

pub unsafe fn do_meta_command(query: &str, table: &mut Table) -> MetaCommandResult {
    match query {
        ".exit" => {
            table.close();
            exit(EXIT_SUCCESS);
        }
        ".btree" => {
            println!("Tree:");
            print_tree(&mut table.pager, 0, 0);
            MetaCommandResult::META_COMMAND_SUCCESS
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
            MetaCommandResult::META_COMMAND_SUCCESS
        }
        _ => MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND,
    }
}

pub enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
}

unsafe fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    let node = get_page(&mut table.pager, table.root_page_num as usize);
    let num_cells = *leaf_node_num_cells(node);

    let row_to_insert = &statement.row_to_insert;
    let key_to_insert = row_to_insert.id;
    let mut cursor = table.find(key_to_insert);

    if cursor.cell_num < num_cells {
        let key_at_index = *leaf_node_key(node, cursor.cell_num);
        if key_at_index == key_to_insert {
            return ExecuteResult::EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(&mut cursor, row_to_insert.id, row_to_insert);
    ExecuteResult::EXECUTE_SUCCESS
}

unsafe fn execute_select(_statement: &Statement, table: &mut Table) -> ExecuteResult {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        let mut row = Row::new();
        deserialize_row(cursor.value(), &mut row);
        print_row(&row);
        cursor.advance();
    }

    ExecuteResult::EXECUTE_SUCCESS
}

pub unsafe fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.r#type {
        StatementType::STATEMENT_INSERT => execute_insert(statement, table),
        StatementType::STATEMENT_SELECT => execute_select(statement, table),
    }
}
