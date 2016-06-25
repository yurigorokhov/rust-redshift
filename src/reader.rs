use std::io::Read;
use std::io::BufReader;

const BUFFER_SIZE: usize = 4096;

pub struct RedshiftRow {

    //TODO: remove public accessor
    pub values: Vec<String>
}

pub struct RedshiftReader<R> {
    reader: R,
    buffer: Box<[u8]>,
    end_of_line: bool,
    pos: usize,
    length: usize,
}

impl<R: Read> RedshiftReader<R> {
    pub fn new(mut reader: R) -> Self {
        RedshiftReader {
            reader: reader,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            end_of_line: false,
            pos: 0,
            length: 0
        }
    }

    pub fn get_next_row(&mut self) -> Option<RedshiftRow> {
        let mut row: Vec<String> = Vec::new();
        loop {
            if let Some(next_item) = self.get_next_item() {
                row.push(next_item);
            } else {
                return if row.len() == 0 { None } else {
                    Some(RedshiftRow { values: row })
                };
            }
        }
    }

    fn get_next_item(&mut self) -> Option<String> {
        if self.end_of_line {

            // previous item was the last one on the row, start a new row
            self.end_of_line = false;
            return None;
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
                            //TODO: proper error
                        }
                        return Some(next_item);
                    }
                    ch @ '\n' | ch @ '\r' => {
                        self.end_of_line = true;
                        if ch == '\r' {
                            let next_next = self.get_next_char(false);
                            if next_next.is_some() && (next_next.unwrap() == '\n') {
                                self.get_next_char(true);
                            }
                        }
                        return Some(next_item);
                    }
                    ' ' => {
                        if next_item.len() == 0 {
                            continue;
                        } else {
                            next_item.push(c);
                        }
                    }
                    '"' => {
                        if next_item.len() == 0 && !found_quote {
                            found_quote = true;
                            continue;
                        } else if next_item.len() > 0 && found_quote {
                            found_closing_quote = true;
                            continue;
                        } else {
                            //TODO: error
                        }
                    }
                    '\\' => {

                        // this is an escape sequence
                        let next_next = self.get_next_char(false);
                        if next_next.is_none() {
                            //TODO: error!
                        }
                        match next_next.unwrap() {
                            '\n' | '\r' | '|' | '\\' | '\'' | '"' => {
                                next_item.push(self.get_next_char(true).unwrap());
                                continue;
                            }
                            _ => {
                                // TODO: error
                            }
                        }
                    }
                    ch @ _ => {

                        // this must be a regular character, add it!
                        next_item.push(ch);
                    }
                }
            } else {


                // we have reached the end of the stream, let's return what we have
                return if next_item.len() > 0 { Some(next_item) } else { None }
            }
        }
    }

    fn get_next_char(&mut self, eat: bool) -> Option<char> {
        if self.pos >= self.length {
            self.length = self.reader.read(&mut self.buffer).unwrap();
            if self.length == 0 {
                self.end_of_line = true;
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


