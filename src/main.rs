mod sqlite;

use std::env;

use anyhow::bail;

use sqlite::{Database, Record, RecordValue};

// Add this function before your main() function
fn format_record_value(value: &RecordValue) -> String {
    match value {
        RecordValue::Text(text) => text.clone(),
        RecordValue::Int(number) => number.to_string(),
        RecordValue::Real(float) => float.to_string(),
        RecordValue::Null => "NULL".to_string(),
        RecordValue::Blob(_) => "[BLOB]".to_string(),
    }
}

// Add this next to your existing format_record_value function
fn matches_where_condition(record: &Record, where_column_pos: usize, expected_value: &str) -> bool {
    let actual_value = &record.values[where_column_pos];
    let actual_string = format_record_value(actual_value);
    actual_string == expected_value
}

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
            let where_pos = parts
                .iter()
                .position(|&word| word.to_lowercase() == "where");

            // let column_name = parts[select_pos + 1];

            let columns_slice = &parts[select_pos + 1..from_pos];
            let columns_string = columns_slice.join(" ");
            let requested_column_names: Vec<&str> =
                columns_string.split(",").map(|s| s.trim()).collect();

            // eprintln!("requested_column_names: {:?}", requested_column_names);

            let table_name = parts[from_pos + 1];

            let where_clause = if let Some(where_position) = where_pos {
                let where_parts = &parts[where_position + 1..];
                Some(where_parts.join(" "))
            } else {
                None
            };

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

            let schema_column_names: Vec<&str> = column_definitions
                .iter()
                .map(|def| def.trim().split_whitespace().next().unwrap())
                .collect();

            let (where_column, where_value, where_column_position) =
                if let Some(where_string) = &where_clause {
                    let where_parts: Vec<&str> = where_string.split_whitespace().collect();
                    if where_parts.len() != 3 {
                        bail!("Invalid WHERE clause format. Expected: column = value");
                    }
                    let where_column = where_parts[0];
                    let where_operator = where_parts[1];
                    let where_value_raw = where_parts[2];
                    if where_operator != "=" {
                        bail!("Only '=' operator is supported in WHERE clauses");
                    }
                    let where_value =
                        if where_value_raw.starts_with('\'') && where_value_raw.ends_with('\'') {
                            &where_value_raw[1..where_value_raw.len() - 1]
                        } else {
                            where_value_raw
                        };

                    let position = schema_column_names
                        .iter()
                        .position(|&schema_col| schema_col == where_column);

                    let position = match position {
                        Some(pos) => pos,
                        None => bail!(
                            "WHERE column '{}' not found in table '{}'",
                            where_column,
                            table_name
                        ),
                    };

                    (Some(where_column), Some(where_value), Some(position))
                } else {
                    (None, None, None)
                };

            // eprintln!("column_names: {:?}", column_names);

            let mut column_positions: Vec<usize> = Vec::new();

            for &requested_column in &requested_column_names {
                let position = schema_column_names
                    .iter()
                    .position(|&schema_col| schema_col == requested_column);

                let position = match position {
                    Some(pos) => pos,
                    None => bail!(
                        "Column '{}' not found in table '{}'",
                        requested_column,
                        table_name
                    ),
                };

                column_positions.push(position);
            }

            // Debug output to verify it works:
            // println!("Column positions: {:?}", column_positions);
            let table_page = db.load_page(db_path, rootpage)?;

            for record in table_page.records() {
                // Bounds checking: ensure record has enough columns
                let max_position = column_positions.iter().max().unwrap_or(&0);
                if record.values.len() <= *max_position {
                    bail!(
                        "Record doesn't have enough columns for position {}",
                        max_position
                    );
                }

                // NEW: WHERE filtering
                if let (Some(_), Some(where_val), Some(where_pos)) =
                    (&where_column, &where_value, &where_column_position)
                {
                    if !matches_where_condition(&record, *where_pos, where_val) {
                        continue; // Skip this record
                    }
                }

                // Existing output logic (unchanged)
                let mut row_values: Vec<String> = Vec::new();

                for &position in &column_positions {
                    let column_value = &record.values[position];
                    let formatted_value = format_record_value(column_value);
                    row_values.push(formatted_value);
                }
                let row_output = row_values.join("|");
                println!("{}", row_output);
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
