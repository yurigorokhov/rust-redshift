#[cfg(test)]
mod test {

    extern crate redshift;

    use std::str;

    struct TestItem<'a> {
        pub A: &'a str,
        pub B: &'a str,
        pub C: &'a str,
    }

    #[test]
    fn basic_writer_test() {

        // Arrange
        let test_column_definitions = vec![
            redshift::writer::ColumnDefinition::<TestItem> {
                name:  "Acolumn",
                extract_column: Box::new(move |i: &TestItem| i.A.to_string().clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {

                name: "Bcolumn",
                extract_column: Box::new(move |i: &TestItem| i.B.to_string().clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {
                name: "Ccolumn",
                extract_column: Box::new(move |i: &TestItem| i.C.to_string().clone()),
            },
        ];
        let items = vec![
            TestItem { A: "a1", B: "b1", C: "c1" },
            TestItem { A: "a2", B: "b2", C: "c2" },
            TestItem { A: "a3", B: "b3", C: "c3" },
        ];

        // Act
        let mut byte_vec: Vec<u8> = Vec::new();
        let mut writer = redshift::writer::Writer::new();
        let res = writer.write_columns(&mut byte_vec, test_column_definitions, items);

        // Assert
        assert!(res.is_ok());
        let result_str = str::from_utf8(&byte_vec).unwrap();
        assert_eq!("\"a1\"|\"b1\"|\"c1\"\n\"a2\"|\"b2\"|\"c2\"\n\"a3\"|\"b3\"|\"c3\"\n", result_str);
    }

    #[test]
    fn basic_writer_with_header() {

        // Arrange
        let test_column_definitions = vec![
            redshift::writer::ColumnDefinition::<TestItem> {
                name:  "Acolumn",
                extract_column: Box::new(move |i: &TestItem| i.A.to_string().clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {

                name: "Bcolumn",
                extract_column: Box::new(move |i: &TestItem| i.B.to_string().clone()),
            },
            redshift::writer::ColumnDefinition::<TestItem> {
                name: "Ccolumn",
                extract_column: Box::new(move |i: &TestItem| i.C.to_string().clone()),
            },
        ];
        let items = vec![
            TestItem { A: "a1", B: "b1", C: "c1" },
            TestItem { A: "a2", B: "b2", C: "c2" },
            TestItem { A: "a3", B: "b3", C: "c3" },
        ];

        // Act
        let mut byte_vec: Vec<u8> = Vec::new();
        let mut writer = redshift::writer::Writer::new();
        let res = writer.with_header().write_columns(&mut byte_vec, test_column_definitions, items);

        // Assert
        assert!(res.is_ok());
        let result_str = str::from_utf8(&byte_vec).unwrap();
        assert_eq!("\"Acolumn\"|\"Bcolumn\"|\"Ccolumn\"\n\"a1\"|\"b1\"|\"c1\"\n\"a2\"|\"b2\"|\"c2\"\n\"a3\"|\"b3\"|\"c3\"\n", result_str);
    }
}
