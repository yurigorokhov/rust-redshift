use std::io::Read;
use std::error::Error;

const BUFFER_SIZE: usize = 4096;

pub struct RedshiftRow {
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
}

impl<R: Read> Iterator for Reader<R> {
    type Item = RedshiftRow;

    fn next(&mut self) -> Option<RedshiftRow> {
        let next_row = self.get_next_row();
        match next_row {
            Ok(maybe_row) => maybe_row,
            Err(e) => panic!(String::from(e.description())),
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

    pub fn get_next_row(&mut self) -> Result<Option<RedshiftRow>, Box<Error>> {
        let mut row: Vec<String> = Vec::new();
        loop {
            match try!(self.get_next_item()) {
                NextItemResult::Item(next_item) => row.push(next_item),
                NextItemResult::EndOfLine => {
                    if row.len() == 0 || (row.len() == 1 && row[0].len() == 0)  {
                        continue;
                    }
                    return Ok(Some(RedshiftRow { values: row }))
                },
                NextItemResult::EndOfStream => {
                    return if row.len() > 0 { Ok(Some(RedshiftRow { values: row })) } else { Ok(None) } },
            }
        }
    }

    fn get_next_item(&mut self) -> Result<NextItemResult, Box<Error>> {
        if self.end_of_stream {
            return Ok(NextItemResult::EndOfStream);
        }
        if self.end_of_line {

            // previous item was the last one on the row, start a new row
            self.end_of_line = false;
            return Ok(NextItemResult::EndOfLine);
        }
        let mut found_quote = false;
        let mut found_closing_quote = false;
        let mut next_item = String::from("");
        loop {
            if let Some(c) = try!(self.get_next_char(true)) {
                match c {
                    '|' => {

                        // end of an item was found
                        if !found_closing_quote && found_quote {
                            return Err(From::from("Did not find a closing quote"));
                        }
                        return Ok(NextItemResult::Item(next_item));
                    },
                    ch @ '\n' | ch @ '\r' => {
                        self.end_of_line = true;
                        if ch == '\r' {
                            let next_next = try!(self.get_next_char(false));
                            if next_next.is_some() && (next_next.unwrap() == '\n') {
                                self.get_next_char(true);
                            }
                        }
                        return Ok(NextItemResult::Item(next_item));
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
                        } else if found_quote {
                            found_closing_quote = true;
                            continue;
                        }
                    },
                    '\\' => {

                        // this is an escape sequence
                        match self.get_next_char(false) {
                            Ok(next_next) => {
                                if next_next.is_none() {
                                    return Err(From::from("Found the beginning of an escape sequence, but no character followed"));
                                }
                                match next_next.unwrap() {
                                    '\n' | '\r' | '|' | '\\' | '\'' | '"' => {
                                        next_item.push(self.get_next_char(true).unwrap().unwrap());
                                        continue;
                                    },
                                    unknown => {
                                        return Err(From::from(format!("Unknown escape sequence \\{}", unknown)));
                                    }
                                }
                            },
                            Err(e) => { return Err(e); },
                        }
                    },
                    ch => {

                        // this must be a regular character, add it!
                        next_item.push(ch);
                    }
                }
            } else {
                return Ok(NextItemResult::Item(next_item));
            }
        }
    }

    fn get_next_char(&mut self, eat: bool) -> Result<Option<char>, Box<Error>> {
        if self.pos >= self.length {
            self.length = try!(self.reader.read(&mut self.buffer));
            if self.length == 0 {
                self.end_of_stream = true;
                return Ok(None);
            }
            self.pos = 0;
        }
        return Ok(Some(
            if eat {
                let c = self.buffer[self.pos as usize];
                self.pos += 1;
                c
            } else { self.buffer[self.pos as usize] } as char
        ));
    }
}


