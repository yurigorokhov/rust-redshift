#[cfg(test)]
mod test {

    extern crate redshift;

    struct TestItem {
        pub A: String,
        pub B: String,
        pub C: String,
    }

    #[test]
    fn basic_writer_test() {

        // Arrange
        let test_column_definitions = vec![
            redshift::writer::ColumnDefinition::<TestItem> {
                name:  "Acolumn".to_string(),
                extract_column: Box::new(move |i: &TestItem| i.A.clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {
                name: "Bcolumn".to_string(),
                extract_column: Box::new(move |i: &TestItem| i.B.clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {
                name: "Ccolumn".to_string(),
                extract_column: Box::new(move |i: &TestItem| i.C.clone()),
            },
        ];
        let items = vec![
            TestItem { A: "a1".to_string(), B: "b1".to_string(), C: "c1".to_string() },
            TestItem { A: "a2".to_string(), B: "b2".to_string(), C: "c2".to_string() },
            TestItem { A: "a3".to_string(), B: "b3".to_string(), C: "c3".to_string() },
        ];

        // Act
        let mut byte_vec: Vec<u8> = Vec::new();
        redshift::writer::write_columns(byte_vec, test_column_definitions, items);
--
        // Assert
    }
}
