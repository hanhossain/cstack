use crate::node::leaf_node_insert;
use crate::repl::{print_constants, print_tree};
use crate::serialization::{deserialize_row, Row, COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
use crate::table::Table;
use libc::{strcpy, EXIT_SUCCESS};
use std::ffi::CString;
use std::process::exit;
use std::str::FromStr;

#[derive(Debug)]
pub enum Statement {
    Insert(Row),
    Select,
}

impl TryFrom<&str> for Statement {
    type Error = PrepareError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if &value[..6] == "insert" {
            unsafe { prepare_insert(value) }
        } else if value == "select" {
            Ok(Statement::Select)
        } else {
            Err(PrepareError::UnrecognizedStatement)
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PrepareError {
    NegativeId,
    StringTooLong,
    SyntaxError,
    UnrecognizedStatement,
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

pub unsafe fn do_meta_command(
    query: &str,
    mut table: Table,
) -> Result<Table, (Table, MetaCommandError)> {
    match query {
        ".exit" => {
            table.close();
            exit(EXIT_SUCCESS);
        }
        ".btree" => {
            println!("Tree:");
            print_tree(&mut table.pager, 0, 0);
            Ok(table)
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
            Ok(table)
        }
        _ => Err((table, MetaCommandError::UnrecognizedCommand)),
    }
}

pub enum ExecuteError {
    DuplicateKey,
}

unsafe fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecuteError> {
    let key_to_insert = row.id;
    let mut cursor = table.find(key_to_insert);

    // The cursor will always point to a leaf node.
    let node = table.pager.page(cursor.page_num as usize).unwrap_leaf();

    if cursor.cell_num < node.num_cells() {
        let key_at_index = node.key(cursor.cell_num);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_too_long() {
        let username: String = std::iter::repeat("a").take(33).collect();
        let email: String = std::iter::repeat("a").take(256).collect();
        let query = format!("insert 1 {} {}", username, email);
        let result = Statement::try_from(query.as_str()).unwrap_err();
        assert_eq!(result, PrepareError::StringTooLong);
    }

    #[test]
    fn id_negative() {
        let query = "insert -1 cstack foo@bar.com";
        let result = Statement::try_from(query).unwrap_err();
        assert_eq!(result, PrepareError::NegativeId);
    }
}
