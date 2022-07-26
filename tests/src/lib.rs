#[cfg(test)]
mod tests {
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
    fn insert_retrieve_row() {
        let db = Database::new();
        let lines = db.run_script(vec![
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ]);
        let expected = vec![
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn table_full_error() {
        let mut input: Vec<_> = (0..1401)
            .map(|i| format!("insert {i} user{i} person{i}@email.com"))
            .collect();
        input.push(String::from(".exit"));
        let db = Database::new();
        let output = db.run_script(input);
        assert_eq!(&output[output.len() - 2], "db > Error: Table full.");
    }

    #[test]
    fn insert_strings_of_max_length() {
        let username = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let email: String = std::iter::repeat("a").take(255).collect();
        let input = vec![
            format!("insert 1 {username} {email}"),
            String::from("select"),
            String::from(".exit"),
        ];
        let db = Database::new();
        let output = db.run_script(input);
        let expected = vec![
            String::from("db > Executed."),
            format!("db > (1, {username}, {email})"),
            String::from("Executed."),
            String::from("db > "),
        ];
        assert_eq!(output, expected);
    }

    #[test]
    fn strings_too_long_error() {
        let username: String = std::iter::repeat("a").take(33).collect();
        let email: String = std::iter::repeat("a").take(256).collect();
        let input = vec![
            format!("insert 1 {username} {email}"),
            String::from("select"),
            String::from(".exit"),
        ];
        let db = Database::new();
        let output = db.run_script(input);
        let expected = vec!["db > String is too long.", "db > Executed.", "db > "];
        assert_eq!(output, expected);
    }

    #[test]
    fn id_negative_error() {
        let input = vec!["insert -1 cstack foo@bar.com", "select", ".exit"];
        let db = Database::new();
        let output = db.run_script(input);
        let expected = vec!["db > ID must be positive.", "db > Executed.", "db > "];
        assert_eq!(output, expected);
    }

    #[test]
    fn close_connection_keep_data() {
        let db = Database::new();
        let output = db.run_script(vec!["insert 1 user1 person1@example.com", ".exit"]);
        let expected = vec!["db > Executed.", "db > "];
        assert_eq!(output, expected);

        let output = db.run_script(vec!["select", ".exit"]);
        let expected = vec!["db > (1, user1, person1@example.com)", "Executed.", "db > "];
        assert_eq!(output, expected);
    }

    #[test]
    fn prints_constants() {
        let db = Database::new();
        let output = db.run_script(vec![".constants", ".exit"]);
        assert_eq!(
            output,
            vec![
                "db > Constants:",
                "ROW_SIZE: 293",
                "COMMON_NODE_HEADER_SIZE: 6",
                "LEAF_NODE_HEADER_SIZE: 10",
                "LEAF_NODE_CELL_SIZE: 297",
                "LEAF_NODE_SPACE_FOR_CELLS: 4086",
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
                "leaf (size 3)",
                "  - 0 : 1",
                "  - 1 : 2",
                "  - 2 : 3",
                "db > ",
            ]
        );
    }

    #[test]
    fn duplicate_id_error() {
        let input = vec![
            "insert 1 user1 person1@example.com",
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ];
        let db = Database::new();
        let output = db.run_script(input);
        let expected = vec![
            "db > Executed.",
            "db > Error: Duplicate key.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ];
        assert_eq!(output, expected);
    }
}
