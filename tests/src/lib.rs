#[cfg(test)]
mod tests {
    use std::env;
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    fn run_script<T: AsRef<str>>(commands: Vec<T>) -> Vec<String> {
        let cstack_path =
            env::var("CSTACK_PATH").expect("missing CSTACK_PATH environment variable");
        let process = Command::new(cstack_path)
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

    #[test]
    fn insert_retrieve_row() {
        let lines = run_script(vec![
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
        let output = run_script(input);
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
        let output = run_script(input);
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
        let output = run_script(input);
        let expected = vec!["db > String is too long.", "db > Executed.", "db > "];
        assert_eq!(output, expected);
    }

    #[test]
    fn id_negative_error() {
        let input = vec!["insert -1 cstack foo@bar.com", "select", ".exit"];
        let output = run_script(input);
        let expected = vec!["db > ID must be positive.", "db > Executed.", "db > "];
        assert_eq!(output, expected);
    }
}
