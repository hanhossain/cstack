use crate::node::leaf_node_insert;
use crate::repl::{print_constants, print_tree};
use crate::serialization::{Row, EMAIL_SIZE, USERNAME_SIZE};
use crate::storage::Storage;
use crate::table::Table;
use crate::Logger;
use libc::EXIT_SUCCESS;
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
            prepare_insert(value)
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

fn prepare_insert(input: &str) -> Result<Statement, PrepareError> {
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

    if username.as_bytes().len() > USERNAME_SIZE || email.as_bytes().len() > EMAIL_SIZE {
        return Err(PrepareError::StringTooLong);
    }

    let row = Row {
        id: id as u32,
        username: username.to_string(),
        email: email.to_string(),
    };

    Ok(Statement::Insert(row))
}

pub enum MetaCommandError {
    UnrecognizedCommand,
}

pub fn do_meta_command<T: Storage>(
    query: &str,
    mut table: Table<T>,
) -> Result<Table<T>, (Table<T>, MetaCommandError)> {
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

#[derive(Debug, PartialEq)]
pub enum ExecuteError {
    DuplicateKey,
}

fn execute_insert<T: Storage>(row: &Row, table: &mut Table<T>) -> Result<(), ExecuteError> {
    let key_to_insert = row.id;
    let cursor = table.find(key_to_insert);

    // The cursor will always point to a leaf node.
    if cursor.cell_num < cursor.node.num_cells() {
        let key_at_index = cursor.node.key(cursor.cell_num);
        if key_at_index == key_to_insert {
            return Err(ExecuteError::DuplicateKey);
        }
    }

    leaf_node_insert(cursor, row.id, row);
    Ok(())
}

fn execute_select<T: Storage, L: Logger>(
    _statement: &Statement,
    table: &mut Table<T>,
    logger: &L,
) -> Result<(), ExecuteError> {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        let row = bincode::deserialize(cursor.value()).unwrap();
        logger.print_row(&row);
        cursor.advance();
    }

    Ok(())
}

pub fn execute_statement<T: Storage, L: Logger>(
    statement: &Statement,
    table: &mut Table<T>,
    logger: &L,
) -> Result<(), ExecuteError> {
    match statement {
        Statement::Insert(row) => execute_insert(row, table),
        Statement::Select => execute_select(statement, table, logger),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryStorageFactory;
    use std::sync::Mutex;

    struct InMemoryLogger {
        logs: Mutex<Vec<String>>,
    }

    impl InMemoryLogger {
        fn new() -> InMemoryLogger {
            InMemoryLogger {
                logs: Mutex::new(Vec::new()),
            }
        }
    }

    impl Logger for InMemoryLogger {
        fn print_row(&self, row: &Row) {
            let mut logs = self.logs.lock().unwrap();
            logs.push(format!("{}", row));
        }
    }

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

    #[test]
    fn select_nothing() {
        let logger = InMemoryLogger::new();
        let mut storage_factory = InMemoryStorageFactory::new();
        let mut table = Table::open(&mut storage_factory, "foobar");
        execute_select(&Statement::Select, &mut table, &logger).unwrap();

        let logs = logger.logs.into_inner().unwrap();
        assert_eq!(logs.len(), 0);
    }

    #[test]
    fn insert_and_select() {
        let logger = InMemoryLogger::new();
        let mut storage_factory = InMemoryStorageFactory::new();
        let mut table = Table::open(&mut storage_factory, "foobar");

        let insert_statement = Statement::try_from("insert 1 a b").unwrap();
        execute_statement(&insert_statement, &mut table, &logger).unwrap();

        execute_select(&Statement::Select, &mut table, &logger).unwrap();
        let logs = logger.logs.into_inner().unwrap();
        assert_eq!(logs, vec!["(1, a, b)"])
    }

    #[test]
    #[should_panic(expected = "Need to implement splitting internal node")]
    fn table_full() {
        let queries: Vec<_> = (0..1401)
            .map(|i| format!("insert {i} user{i} person{i}@email.com"))
            .collect();
        let logger = InMemoryLogger::new();
        let mut storage_factory = InMemoryStorageFactory::new();
        let mut table = Table::open(&mut storage_factory, "foobar");

        for query in &queries {
            let statement = Statement::try_from(query.as_str()).unwrap();
            execute_statement(&statement, &mut table, &logger).unwrap();
        }
    }

    #[test]
    fn insert_duplicate_id() {
        let statement = Statement::try_from("insert 1 foo bar").unwrap();

        let logger = InMemoryLogger::new();
        let mut storage_factory = InMemoryStorageFactory::new();
        let mut table = Table::open(&mut storage_factory, "foobar");

        execute_statement(&statement, &mut table, &logger).unwrap();
        let error = execute_statement(&statement, &mut table, &logger).unwrap_err();
        assert_eq!(error, ExecuteError::DuplicateKey);
    }

    #[test]
    fn insert_strings_of_max_length() {
        let username = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let email: String = std::iter::repeat("a").take(255).collect();
        let query = format!("insert 1 {} {}", username, email);
        let statement = Statement::try_from(query.as_str()).unwrap();

        let logger = InMemoryLogger::new();
        let mut storage_factory = InMemoryStorageFactory::new();
        let mut table = Table::open(&mut storage_factory, "foobar");

        execute_statement(&statement, &mut table, &logger).unwrap();
        execute_statement(&Statement::Select, &mut table, &logger).unwrap();

        let logs = logger.logs.into_inner().unwrap();
        assert_eq!(logs, vec![format!("(1, {}, {})", username, email)]);
    }

    #[test]
    fn keep_data_after_close() {
        let statement = Statement::try_from("insert 1 foo bar").unwrap();
        let mut storage_factory = InMemoryStorageFactory::new();

        {
            let logger = InMemoryLogger::new();
            let mut table = Table::open(&mut storage_factory, "foobar");
            execute_statement(&statement, &mut table, &logger).unwrap();
            table.close();
        }

        {
            let logger = InMemoryLogger::new();
            let mut table = Table::open(&mut storage_factory, "foobar");
            execute_statement(&Statement::Select, &mut table, &logger).unwrap();

            let logs = logger.logs.into_inner().unwrap();
            assert_eq!(logs, vec!["(1, foo, bar)"]);
        }
    }
}
