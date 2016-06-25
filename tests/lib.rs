#[cfg(test)]
mod test {

    extern crate redshift;
    use std::io::BufReader;

    #[test]
    fn basic_reader_test() {

        // Arrange
        let input_data =
b"\"a\"|\"b\"
\"c\"|\"d\"\n";
        let mut reader = BufReader::new(&input_data[..]);

        // Act
        let mut redshift_reader = redshift::reader::RedshiftReader::new(&mut reader);

        // Assert
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["a", "b"]);
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["c", "d"]);
        assert!(redshift_reader.get_next_row().is_none());
    }

    #[test]
    fn can_parse_empty_values() {

        // Arrange
        let input_data =
b"||\n||\n";
        let mut reader = BufReader::new(&input_data[..]);

        // Act
        let mut redshift_reader = redshift::reader::RedshiftReader::new(&mut reader);

        // Assert
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["", "", ""]);
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["", "", ""]);
        assert!(redshift_reader.get_next_row().is_none());
    }

    #[test]
    fn basic_escape_sequences_test() {

        // Arrange
        let input_data =
b"\"tobe\\|nottobe\"|\"iama\\\"doublequote\"
\"iama\\'singlequote\"|\"iama\\\\backslash\"
\"iama\\|verticalbar\"|iama\\\nnewline\"
\"iama\\\rcarriagereturn\"|\"iam\\\r\\\nwindows\"";
        let mut reader = BufReader::new(&input_data[..]);

        // Act
        let mut redshift_reader = redshift::reader::RedshiftReader::new(&mut reader);

        // Assert
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["tobe|nottobe", "iama\"doublequote"]);
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["iama'singlequote", "iama\\backslash"]);
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["iama|verticalbar", "iama\nnewline"]);
        validate_row(redshift_reader.get_next_row().unwrap(), vec!["iama\rcarriagereturn", "iam\r\nwindows"]);
        assert!(redshift_reader.get_next_row().is_none());
    }

    fn validate_row(row: redshift::reader::RedshiftRow, expected: Vec<&str>) {
        assert_eq!(expected.len(), row.values.len());
        for i in 0..expected.len() {
            assert_eq!(String::from(expected[i]), row.values[i]);
        }
    }
}
