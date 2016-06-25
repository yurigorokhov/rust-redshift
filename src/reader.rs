use std::io::Read;

const BUFFER_SIZE: usize = 4096;

//TODO: make of trait error
pub struct RedshiftParseError {
    message: String
}

pub struct RedshiftRow {

    //TODO: remove public accessor
    pub values: Vec<String>
}

pub struct Reader<R> {
    reader: R,
    buffer: Box<[u8]>,
    end_of_line: bool,
    end_of_stream: bool,
    pos: usize,
    length: usize,
}

enum NextItemResult {
    EndOfLine,
    EndOfStream,
    Item(String),
    Error(RedshiftParseError),
}

impl<R: Read> Iterator for Reader<R> {
    type Item = RedshiftRow;

    fn next(&mut self) -> Option<RedshiftRow> {
        let next_row = self.get_next_row();
        match next_row {
            Ok(maybe_row) => maybe_row,
            Err(e) => panic!(e.message),
        }
    }
}

impl<R: Read> Reader<R> {

    pub fn new(reader: R) -> Self {
        Reader {
            reader: reader,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            end_of_line: false,
            end_of_stream: false,
            pos: 0,
            length: 0
        }
    }

    pub fn get_next_row(&mut self) -> Result<Option<RedshiftRow>, RedshiftParseError> {
        let mut row: Vec<String> = Vec::new();
        loop {
            match self.get_next_item() {
                NextItemResult::Item(next_item) => row.push(next_item),
                NextItemResult::EndOfLine => return Ok(Some(RedshiftRow { values: row })),
                NextItemResult::EndOfStream => return if row.len() > 0 { Ok(Some(RedshiftRow { values: row })) } else { Ok(None) },
                NextItemResult::Error(e) => return Err(e),
            }
        }
    }

    fn get_next_item(&mut self) -> NextItemResult {
        if self.end_of_stream {
            return NextItemResult::EndOfStream;
        }
        if self.end_of_line {

            // previous item was the last one on the row, start a new row
            self.end_of_line = false;
            return NextItemResult::EndOfLine;
        }
        let mut found_quote = false;
        let mut found_closing_quote = false;
        let mut next_item = String::from("");
        loop {
            if let Some(c) = self.get_next_char(true) {
                match c {
                    '|' => {

                        // end of an item was found
                        if !found_closing_quote && found_quote {
                            return NextItemResult::Error(RedshiftParseError{ message: String::from("Did not find a closing quote") });
                        }
                        return NextItemResult::Item(next_item);
                    },
                    ch @ '\n' | ch @ '\r' => {
                        self.end_of_line = true;
                        if ch == '\r' {
                            let next_next = self.get_next_char(false);
                            if next_next.is_some() && (next_next.unwrap() == '\n') {
                                self.get_next_char(true);
                            }
                        }
                        return NextItemResult::Item(next_item);
                    },
                    ' ' => {
                        if next_item.len() == 0 {
                            continue;
                        } else {
                            next_item.push(c);
                        }
                    },
                    '"' => {
                        //TODO: does this handle "" case?
                        if next_item.len() == 0 && !found_quote {
                            found_quote = true;
                            continue;
                        } else if next_item.len() > 0 && found_quote {
                            found_closing_quote = true;
                            continue;
                        } else {
                            return NextItemResult::Error(RedshiftParseError {
                                message: String::from("Found invalid quote that was not escaped")
                            });
                        }
                    },
                    '\\' => {

                        // this is an escape sequence
                        let next_next = self.get_next_char(false);
                        if next_next.is_none() {
                            return NextItemResult::Error(RedshiftParseError {
                                message: String::from("Found the beginning of an escape sequence, but no character followed")
                            });
                        }
                        match next_next.unwrap() {
                            '\n' | '\r' | '|' | '\\' | '\'' | '"' => {
                                next_item.push(self.get_next_char(true).unwrap());
                                continue;
                            },
                            unknown => {
                                return NextItemResult::Error(RedshiftParseError {
                                    message: String::from(format!("Unknown escape sequence \\{}", unknown))
                                });
                            }
                        }
                    },
                    ch => {

                        // this must be a regular character, add it!
                        next_item.push(ch);
                    }
                }
            } else {
                return NextItemResult::Item(next_item)
            }
        }
    }

    fn get_next_char(&mut self, eat: bool) -> Option<char> {
        if self.pos >= self.length {
            self.length = self.reader.read(&mut self.buffer).unwrap();
            if self.length == 0 {
                self.end_of_stream = true;
                return None;
            }
        }
        return Some(
            if eat {
                let c = self.buffer[self.pos as usize];
                self.pos += 1;
                c
            } else { self.buffer[self.pos as usize] } as char
        );
    }
}


