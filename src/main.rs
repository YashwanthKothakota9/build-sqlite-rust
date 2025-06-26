mod sqlite;

use std::env;

use anyhow::bail;

use sqlite::{Database, RecordValue};

fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }
    let (db_path, command) = (&args[1], &args[2]);

    let db = Database::load(db_path)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size);
            println!("number of tables: {}", db.root_page.cell_pointers.len());
        }
        ".tables" => {
            let table_names: Vec<_> = db
                .root_page
                .records()
                .map(|r| match &r.values[2] {
                    RecordValue::Text(s) => s.clone(),
                    _ => panic!("Expected table name, got {:?}", r.values[0]),
                })
                .collect();
            let table_names = table_names.join(" ");

            println!("{}", table_names);
        }
        command if command.starts_with("select count(*) from") => {
            let parts: Vec<&str> = command.split_whitespace().collect();
            let table_name = parts.last().unwrap();

            let table_record = db
                .root_page
                .records()
                .find(|record| match &record.values[1] {
                    RecordValue::Text(name) => name == table_name,
                    _ => false,
                });
            let table_record = match table_record {
                Some(record) => record,
                None => bail!("Table not found: {}", table_name),
            };
            let root_page = match &table_record.values[3] {
                RecordValue::Int(page_num) => *page_num as usize,
                _ => bail!("Invalid root page value for table: {}", table_name),
            };
            let table_page = db.load_page(db_path, root_page)?;

            let count = table_page.records().count();
            println!("{}", count);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
