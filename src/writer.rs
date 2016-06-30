use std::error::Error;
use std::io::Write;

/// Structure to define column name and and column value
///
/// # Example
/// ```
/// extern crate redshift;
/// use std::str;
///
/// struct TestItem<'a> {
///     pub A: &'a str,
///     pub B: &'a str,
///     pub C: &'a str,
/// }
///
/// fn main() {
///     let test_column_definitions = vec![
///         redshift::writer::ColumnDefinition::<TestItem> {
///             name:  "Acolumn",
///             extract_column: Box::new(move |i: &TestItem| i.A.to_string().clone()),
///         },
///         redshift::writer::ColumnDefinition::<TestItem> {
///             name: "Bcolumn",
///             extract_column: Box::new(move |i: &TestItem| i.B.to_string().clone()),
///         },
///         redshift::writer::ColumnDefinition::<TestItem> {
///             name: "Ccolumn",
///             extract_column: Box::new(move |i: &TestItem| i.C.to_string().clone()),
///         },
///     ];
/// }
/// ```
pub struct ColumnDefinition<'a, T> {

    /// Column name
    pub name: &'a str,

    /// Method to extract the column value from item `T`
    pub extract_column: Box<(Fn (&T) -> String)>,
}

/// Writer for redshift files
///
/// # Examples
/// ```
/// extern crate redshift;
/// 
/// use std::str;
/// 
/// struct TestItem<'a> {
///     pub A: &'a str,
///     pub B: &'a str,
///     pub C: &'a str,
/// }
/// 
/// fn main() {
/// 
///     // Arrange
///     let test_column_definitions = vec![
///         redshift::writer::ColumnDefinition::<TestItem> {
///             name:  "Acolumn",
///             extract_column: Box::new(move |i: &TestItem| i.A.to_string().clone()),
///         },
///         redshift::writer::ColumnDefinition::<TestItem> {
/// 
///             name: "Bcolumn",
///             extract_column: Box::new(move |i: &TestItem| i.B.to_string().clone()),
///         },
///         redshift::writer::ColumnDefinition::<TestItem> {
///             name: "Ccolumn",
///             extract_column: Box::new(move |i: &TestItem| i.C.to_string().clone()),
///         },
///     ];
///     let items = vec![
///         TestItem { A: "a1", B: "b1", C: "c1" },
///         TestItem { A: "a2", B: "b2", C: "c2" },
///         TestItem { A: "a3", B: "b3", C: "c3" },
///     ];
/// 
///     // Act
///     let mut byte_vec: Vec<u8> = Vec::new();
///     let mut writer = redshift::writer::Writer::new();
///     let res = writer.write_columns(&mut byte_vec, test_column_definitions, items);
/// }
/// ```
pub struct Writer {
    output_header: bool,
}

impl Writer {

    /// Construct a new writer
    ///
    /// # Examples
    /// ```
    /// extern crate redshift;
    /// use std::str;
    ///
    /// let mut writer = redshift::writer::Writer::new();
    ///
    /// ```
    pub fn new<'a>() -> Writer {
        Writer {
            output_header: false,
        }
    }


    /// Construct a new writer
    ///
    /// # Examples
    /// ```
    /// extern crate redshift;
    /// use std::str;
    ///
    /// let mut writer = redshift::writer::Writer::new();
    /// let mut writer_with_header = writer.with_header();
    /// ```
    pub fn with_header(&mut self) -> &mut Writer {
        self.output_header = true;
        self
    }

    /// Write out rows to a writer `W` using the provided column definitions and item list
    pub fn write_columns<W: Write, T>(&self, mut writer: W, column_definitions: Vec<ColumnDefinition<T>>, items: Vec<T>) -> Result<(), Box<Error>> {

        // write headers
        if self.output_header {
            for i in 0..column_definitions.len() {
                let escaped = escape_column_value(String::from(column_definitions[i].name));
                try!(writer.write(escaped.as_bytes()));
                if i < column_definitions.len() - 1 {
                    try!(writer.write(b"|"));
                }
            }
            try!(writer.write(b"\n"));
        }

        // encode each item and write it out to the stream
        for item in &items {

            // for each column, extract the column value from the item
            for i in 0..column_definitions.len() {
                let escaped = escape_column_value((column_definitions[i].extract_column)(item));
                try!(writer.write(escaped.as_bytes()));
                if i < column_definitions.len() - 1 {
                    try!(writer.write(b"|"));
                }
            }
            try!(writer.write(b"\n"));
            try!(writer.flush());
        }
        Ok(())
    }
}

fn escape_column_value(value: String) -> String {
    let escaped = value
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\n", "\\\n")
        .replace("\r", "\\\r")
        .replace("'", "\'")
        .replace("\"", "\\\"");
    return format!("\"{}\"", escaped);
}
