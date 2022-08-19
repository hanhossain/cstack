use std::env;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

struct Database {
    filename: PathBuf,
    cstack_path: String,
}

impl Database {
    fn new() -> Database {
        let cstack_path =
            env::var("CSTACK_PATH").expect("missing CSTACK_PATH environment variable");

        let filename = uuid::Uuid::new_v4().to_string();
        let mut path = std::path::PathBuf::from(&filename);
        path.set_extension("db");

        Database {
            filename: path,
            cstack_path,
        }
    }

    fn run_script<T: AsRef<str>>(&self, commands: Vec<T>) -> Vec<String> {
        let process = Command::new(&self.cstack_path)
            .arg(&self.filename)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let mut input = String::new();
        for command in commands {
            input.push_str(command.as_ref());
            input.push('\n');
        }

        process.stdin.unwrap().write_all(input.as_bytes()).unwrap();

        let mut string = String::new();
        process.stdout.unwrap().read_to_string(&mut string).unwrap();
        string.lines().map(|l| l.to_string()).collect()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        std::fs::remove_file(&self.filename).unwrap();
    }
}

#[test]
fn prints_constants() {
    let db = Database::new();
    let output = db.run_script(vec![".constants", ".exit"]);
    assert_eq!(
        output,
        vec![
            "db > Constants:",
            "ROW_SIZE: 299",
            "COMMON_NODE_HEADER_SIZE: 8",
            "LEAF_NODE_HEADER_SIZE: 16",
            "LEAF_NODE_CELL_SIZE: 303",
            "LEAF_NODE_SPACE_FOR_CELLS: 4080",
            "LEAF_NODE_MAX_CELLS: 13",
            "db > ",
        ]
    );
}

#[test]
fn prints_structure_of_one_node_btree() {
    let db = Database::new();
    let input = vec![
        "insert 3 user3 person3@example.com",
        "insert 1 user1 person1@example.com",
        "insert 2 user2 person2@example.com",
        ".btree",
        ".exit",
    ];
    let output = db.run_script(input);
    assert_eq!(
        output,
        vec![
            "db > Executed.",
            "db > Executed.",
            "db > Executed.",
            "db > Tree:",
            "- leaf (size 3)",
            "  - 1",
            "  - 2",
            "  - 3",
            "db > ",
        ]
    );
}

#[test]
fn prints_structure_of_three_node_btree() {
    let db = Database::new();
    let mut input: Vec<_> = (1..=14)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(String::from(".btree"));
    input.push(String::from("insert 15 user15 person15@example.com"));
    input.push(String::from(".exit"));
    let output = db.run_script(input);
    assert_eq!(
        &output[14..],
        &vec![
            "db > Tree:",
            "- internal (size 1)",
            "  - leaf (size 7)",
            "    - 1",
            "    - 2",
            "    - 3",
            "    - 4",
            "    - 5",
            "    - 6",
            "    - 7",
            "  - key 7",
            "  - leaf (size 7)",
            "    - 8",
            "    - 9",
            "    - 10",
            "    - 11",
            "    - 12",
            "    - 13",
            "    - 14",
            "db > Executed.",
            "db > ",
        ]
    )
}

#[test]
fn print_all_rows_in_a_multi_level_tree() {
    let mut input: Vec<_> = (1..=15)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(String::from("select"));
    input.push(String::from(".exit"));
    let db = Database::new();
    let output = db.run_script(input);
    let mut expected = vec![String::from("db > (1, user1, person1@example.com)")];
    for i in 2..=15 {
        expected.push(format!("({i}, user{i}, person{i}@example.com)"));
    }
    expected.push(String::from("Executed."));
    expected.push(String::from("db > "));
    assert_eq!(&output[15..], &expected);
}
