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
        command
            if command.to_lowercase().starts_with("select")
                && command.to_lowercase().contains("from") =>
        {
            let parts: Vec<&str> = command.split_whitespace().collect();

            let select_pos = parts
                .iter()
                .position(|&word| word.to_lowercase() == "select")
                .unwrap();
            let from_pos = parts
                .iter()
                .position(|&word| word.to_lowercase() == "from")
                .unwrap();

            let column_name = parts[select_pos + 1];
            let table_name = parts[from_pos + 1];

            // eprintln!("column_name: {}", column_name);
            // eprintln!("table_name: {}", table_name);

            let table_record = db
                .root_page
                .records()
                .find(|record| match &record.values[1] {
                    RecordValue::Text(name) => name == table_name,
                    _ => false,
                });

            let table_record = match table_record {
                Some(record) => record,
                None => bail!("Table '{}' not found", table_name),
            };
            // Extract CREATE TABLE statement
            let create_table_sql = match &table_record.values[4] {
                RecordValue::Text(sql) => sql,
                _ => bail!("Invalid CREATE TABLE statement for table '{}'", table_name),
            };

            // Extract rootpage
            let rootpage = match &table_record.values[3] {
                RecordValue::Int(page_num) => *page_num as usize,
                _ => bail!("Invalid rootpage value for table '{}'", table_name),
            };

            // eprintln!("CREATE TABLE SQL: {}", create_table_sql);

            let start = create_table_sql.find('(').unwrap() + 1;
            // let end = create_table_sql.rfind(')').unwrap();
            let columns_part = &create_table_sql[start..];

            // eprintln!("columns_part: {}", columns_part);

            let column_definitions: Vec<&str> = columns_part.split(',').collect();

            // eprintln!("column_definitions: {:?}", column_definitions);

            let column_names: Vec<&str> = column_definitions
                .iter()
                .map(|def| def.trim().split_whitespace().next().unwrap())
                .collect();

            // eprintln!("column_names: {:?}", column_names);

            let column_position = column_names.iter().position(|&name| name == column_name);

            let column_position = match column_position {
                Some(pos) => pos,
                None => bail!(
                    "Column '{}' not found in table '{}'",
                    column_name,
                    table_name
                ),
            };

            let table_page = db.load_page(db_path, rootpage)?;

            for record in table_page.records() {
                if record.values.len() <= column_position {
                    bail!(
                        "Record doesn't have enough columns for position {}",
                        column_position
                    );
                }
                let column_value = &record.values[column_position];

                match column_value {
                    RecordValue::Text(text) => println!("{}", text),
                    RecordValue::Int(number) => println!("{}", number),
                    RecordValue::Real(float) => println!("{}", float),
                    RecordValue::Null => println!("NULL"),
                    RecordValue::Blob(_) => println!("[BLOB]"),
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
