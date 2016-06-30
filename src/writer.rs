use std::error::Error;
use std::io::Write;

pub struct ColumnDefinition<'a, T> {
    pub name: &'a str,
    pub extract_column: Box<(Fn (&T) -> String)>,
}

pub struct Writer {
    output_header: bool,
}

impl Writer {

    pub fn new<'a>() -> Writer {
        Writer {
            output_header: false,
        }
    }

    pub fn with_header(&mut self) -> &mut Writer {
        self.output_header = true;
        self
    }

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
