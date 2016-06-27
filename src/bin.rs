extern crate csv;
extern crate redshift;

use std::io;

pub fn main() {

    // parse redshift file from stdin
    let redshift_reader = redshift::reader::Reader::new(io::stdin());

    // create a writer to stdout
    let mut csv_writer = csv::Writer::from_writer(io::stdout());

    // write out each record
    for row in redshift_reader {
        csv_writer.encode(row.values).unwrap();
    }
}
