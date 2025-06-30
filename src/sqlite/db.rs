//! # sqlite/db.rs – How we turn raw bytes into neat rows
//!
//! ```text
//!  .db file  --read-->  Page  --decode-->  Record(s)
//!                      ▲  traverse  |
//!                      └────────────┘
//! ```
//!
//! – *Page*  = one chunk inside the file (often 4 KB).  
//! – *Record* = one row from a table.
//!
//! The code is split into:
//! 1. `Page` – low-level decoding helpers.
//! 2. `Database` – high-level walkers that collect rows.
//!
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

const DB_HEADER_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub enum RecordValue {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    #[allow(dead_code)]
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct Record {
    #[allow(dead_code)]
    pub id: u64,
    pub values: Vec<RecordValue>,
}

#[derive(Debug)]
pub enum PageType {
    TableLeaf,
    TableInterior,
    IndexLeaf,
    IndexInterior,
}

#[derive(Debug)]
pub struct Page {
    #[allow(dead_code)]
    pub typ: PageType,
    pub cell_pointers: Vec<usize>,
    pub right_most_child: Option<u32>,
    data: Vec<u8>,
}

impl Page {
    fn from_data(page_size: u16, data: Vec<u8>) -> Self {
        let typ = match data[0] {
            13 => PageType::TableLeaf,
            5 => PageType::TableInterior,
            10 => PageType::IndexLeaf,
            2 => PageType::IndexInterior,
            _ => panic!("Invalid page type: {}", data[0]),
        };

        let right_most_child = match typ {
            PageType::TableInterior | PageType::IndexInterior => {
                // Bytes 8-11 contain the rightmost child page number for interior pages
                Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]]))
            }
            PageType::TableLeaf => None,
            _ => None,
        };

        let cell_count = u16::from_be_bytes([data[3], data[4]]);
        let data_offset = match typ {
            PageType::TableInterior | PageType::IndexInterior => {
                12 + (cell_count as usize * 2) // Interior pages: 12-byte header
            }
            PageType::TableLeaf | PageType::IndexLeaf => {
                8 + (cell_count as usize * 2) // Leaf pages: 8-byte header
            }
        };
        let offset = (page_size as usize) - data.len() + data_offset;

        let cell_pointer_start = match typ {
            PageType::TableInterior | PageType::IndexInterior => 12, // Interior pages start at 12
            PageType::TableLeaf | PageType::IndexLeaf => 8,          // Leaf pages start at 8
        };

        let cell_pointers = data[cell_pointer_start..data_offset]
            .chunks(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]) as usize - offset)
            .collect();

        let data = data[data_offset..].to_vec();

        Self {
            typ,
            cell_pointers,
            right_most_child,
            data,
        }
    }

    // ------------------------------------------------------------
    // Helper: read a SQLite *varint* (1-9 byte variable-length int)
    // ------------------------------------------------------------
    // Rules recap in kid-speak:
    // • Only the lowest 7 bits of each byte belong to the number.
    // • The highest bit (0x80) tells us if more bytes follow (1 = yes).
    // • We keep shifting our previous bits left by 7 and add the new 7.
    fn get_varint(data: &[u8], offset: &mut usize) -> u64 {
        let mut value = 0u64;
        for (i, byte) in data[*offset..].iter().enumerate() {
            value <<= 7;
            value += (byte & 0x7F) as u64;
            if byte & 0x80 == 0 {
                *offset += i + 1;
                return value;
            }
        }
        *offset += data.len();
        value
    }

    // -------------------------------------------------------------------
    // Helper: take the next `n` bytes and right-align them inside 8 bytes
    //          so that Rust can turn them into i64 / f64 with one call.
    // -------------------------------------------------------------------
    fn get_be_bytes(
        n: usize,
        data: &mut impl Iterator<Item = u8>,
    ) -> Result<[u8; 8], &'static str> {
        let mut bytes = [0; 8];
        for i in 0..n {
            match data.next() {
                Some(byte) => bytes[8 - n + i] = byte,
                None => return Err("Unexpected end of data while reading bytes"),
            }
        }
        Ok(bytes)
    }

    /// Parse a SQLite record (starting at the *header size* varint) and return
    /// (values, bytes_consumed).
    /// This helper is shared by table and index cell parsing.
    fn parse_record_values(data: &[u8]) -> (Vec<RecordValue>, usize) {
        let mut local_offset = 0;

        // 1. header size varint
        let header_size = Self::get_varint(data, &mut local_offset) as usize;

        // 2. The header area follows immediately after the varint we just read.
        let header_start = local_offset;
        let header_end = header_start + header_size - 1; // -1 because header size includes itself

        // 3. Values segment starts right after the header area
        let mut values_iter = data[header_end..].iter().copied();

        let mut values = Vec::new();
        let mut header_offset = 0;

        // Iterate over serial types in header area
        while header_offset < header_size - 1 {
            let serial_type = Self::get_varint(&data[header_start..header_end], &mut header_offset);

            let value = match serial_type {
                0 => RecordValue::Null,
                1..=6 => {
                    let n = match serial_type {
                        5 => 6,
                        6 => 8,
                        v => v,
                    } as usize;
                    match Self::get_be_bytes(n, &mut values_iter) {
                        Ok(bytes) => RecordValue::Int(i64::from_be_bytes(bytes)),
                        Err(_) => return (values, header_end + header_offset),
                    }
                }
                7 => match Self::get_be_bytes(8, &mut values_iter) {
                    Ok(bytes) => RecordValue::Real(f64::from_be_bytes(bytes)),
                    Err(_) => return (values, header_end + header_offset),
                },
                8 => RecordValue::Int(0),
                9 => RecordValue::Int(1),
                serial if serial >= 12 => {
                    let length = ((serial - 12) / 2) as usize;
                    let bytes: Vec<u8> = (&mut values_iter).take(length).collect();
                    if serial % 2 == 0 {
                        RecordValue::Blob(bytes)
                    } else {
                        RecordValue::Text(String::from_utf8_lossy(&bytes).to_string())
                    }
                }
                _ => panic!("Invalid serial type: {}", serial_type),
            };

            values.push(value);
        }

        // Exact byte count isn't vital for current callers; we return header_end
        // (bytes up to the start of the values segment) as an approximation.
        (values, header_end)
    }

    fn get_record(&self, pointer: usize) -> Record {
        let mut offset = pointer;
        let size = Self::get_varint(&self.data, &mut offset) as usize;
        let id = Self::get_varint(&self.data, &mut offset) as u64;

        // Delegate to common parser for record values
        let (values, _consumed) = Self::parse_record_values(&self.data[offset..]);

        Record { id, values }
    }

    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        self.cell_pointers.iter().map(|i| self.get_record(*i))
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self.typ, PageType::TableLeaf | PageType::IndexLeaf)
    }

    pub fn get_child_pages(&self) -> Vec<u32> {
        match self.typ {
            PageType::TableLeaf => Vec::new(), // Leaf pages have no children
            PageType::TableInterior | PageType::IndexInterior => {
                let mut child_pages = Vec::new();

                // Each cell in an interior page contains a child page number
                for &pointer in &self.cell_pointers {
                    // Interior page cell format: [4-byte child page][varint key]
                    if pointer + 4 <= self.data.len() {
                        let child_page = u32::from_be_bytes([
                            self.data[pointer],
                            self.data[pointer + 1],
                            self.data[pointer + 2],
                            self.data[pointer + 3],
                        ]);
                        child_pages.push(child_page);
                    }
                }

                // Don't forget the rightmost child!
                if let Some(rightmost) = self.right_most_child {
                    child_pages.push(rightmost);
                }

                child_pages
            }
            _ => Vec::new(), // Leaf pages have no children (TableLeaf & IndexLeaf)
        }
    }

    // ---------------- Index-specific helpers ----------------

    /// Parse a cell in an **index leaf** page (page type 0x0A) and return `(country, rowid)`.
    fn get_index_leaf_entry(&self, pointer: usize) -> (String, u64) {
        let mut offset = pointer;

        // First varint: payload size (we can ignore the exact value)
        let _payload_size = Self::get_varint(&self.data, &mut offset) as usize;

        // Next bytes start the record (header size varint comes first).
        let (values, _) = Self::parse_record_values(&self.data[offset..]);

        if values.len() != 2 {
            panic!(
                "Index leaf record expected 2 columns (country, rowid), got {}",
                values.len()
            );
        }

        let country = match &values[0] {
            RecordValue::Text(s) => s.clone(),
            _ => panic!("Expected TEXT in first column of index record"),
        };

        let rowid = match &values[1] {
            RecordValue::Int(n) => *n as u64,
            _ => panic!("Expected INT rowid in second column of index record"),
        };

        (country, rowid)
    }

    /// Parse a cell in an **index interior** page (page type 0x02).
    /// Returns `(country_key, child_page)`
    fn get_index_interior_entry(&self, pointer: usize) -> (String, u32) {
        // After the 4-byte child pointer comes a varint for payload size
        let mut offset = pointer + 4;
        let _payload_size = Self::get_varint(&self.data, &mut offset);

        // Now `offset` points at the start of the record header-size varint
        let (values, _) = Self::parse_record_values(&self.data[offset..]);

        if values.is_empty() {
            panic!("Index interior record expected at least 1 column (country key)");
        }

        let country = match &values[0] {
            RecordValue::Text(s) => s.clone(),
            RecordValue::Int(n) => n.to_string(),
            RecordValue::Real(f) => f.to_string(),
            RecordValue::Null => "NULL".to_string(),
            RecordValue::Blob(_) => "[BLOB]".to_string(),
        };

        let child_page = u32::from_be_bytes([
            self.data[pointer],
            self.data[pointer + 1],
            self.data[pointer + 2],
            self.data[pointer + 3],
        ]);

        (country, child_page)
    }

    /// Convenience iterator over index leaf entries (only valid for IndexLeaf pages).
    pub fn index_leaf_entries(&self) -> Vec<(String, u64)> {
        if !matches!(self.typ, PageType::IndexLeaf) {
            panic!("Called index_leaf_entries on non-index-leaf page");
        }
        self.cell_pointers
            .iter()
            .map(|&ptr| self.get_index_leaf_entry(ptr))
            .collect()
    }

    /// Returns vector of `(country_key, child_page)` for index interior page.
    pub fn index_interior_entries(&self) -> Vec<(String, u32)> {
        if !matches!(self.typ, PageType::IndexInterior) {
            panic!("Called index_interior_entries on non-index-interior page");
        }
        self.cell_pointers
            .iter()
            .map(|&ptr| self.get_index_interior_entry(ptr))
            .collect()
    }

    // ---------------- Table interior helpers (rowid keys) ----------------

    /// Return (child_page, rowid_key) for a cell in a **table interior** page.
    fn get_table_interior_entry(&self, pointer: usize) -> (u32, u64) {
        let child_page = u32::from_be_bytes([
            self.data[pointer],
            self.data[pointer + 1],
            self.data[pointer + 2],
            self.data[pointer + 3],
        ]);

        let mut offset = pointer + 4;
        let rowid_key = Self::get_varint(&self.data, &mut offset);

        (child_page, rowid_key)
    }

    /// Convenience to iterate interior table entries
    pub fn table_interior_entries(&self) -> Vec<(u32, u64)> {
        if !matches!(self.typ, PageType::TableInterior) {
            panic!("Called table_interior_entries on non-table-interior page");
        }
        self.cell_pointers
            .iter()
            .map(|&ptr| self.get_table_interior_entry(ptr))
            .collect()
    }
}

#[derive(Debug)]
pub struct Database {
    pub page_size: u16,
    pub root_page: Page,
}

impl Database {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let mut file = File::open(path)?;

        let mut db_header = [0; DB_HEADER_SIZE];
        file.read_exact(&mut db_header)?;
        let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

        let mut root_page = vec![0; page_size as usize - DB_HEADER_SIZE];
        file.read_exact(&mut root_page)?;
        let root_page = Page::from_data(page_size, root_page);

        Ok(Self {
            page_size,
            root_page,
        })
    }

    pub fn load_page(&self, path: &str, page_number: usize) -> anyhow::Result<Page> {
        // Validate page number
        if page_number == 0 {
            anyhow::bail!("Invalid page number: page numbers start from 1");
        }

        let mut file = File::open(path)?;

        // Calculate correct page offset
        let page_offset = if page_number == 1 {
            0 // Page 1 starts at offset 0
        } else {
            (page_number - 1) * (self.page_size as usize)
        };

        // Calculate how much data to read
        let (read_offset, page_data_size) = if page_number == 1 {
            (DB_HEADER_SIZE, self.page_size as usize - DB_HEADER_SIZE)
        } else {
            (0, self.page_size as usize)
        };

        file.seek(SeekFrom::Start((page_offset + read_offset) as u64))?;
        let mut page_data = vec![0; page_data_size];
        file.read_exact(&mut page_data)?;

        Ok(Page::from_data(self.page_size, page_data))
    }

    pub fn get_all_records(
        &self,
        db_path: &str,
        root_page_num: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let mut all_records = Vec::new();
        self.traverse_btree(db_path, root_page_num, &mut all_records)?;
        Ok(all_records)
    }

    fn traverse_btree(
        &self,
        db_path: &str,
        page_num: usize,
        records: &mut Vec<Record>,
    ) -> anyhow::Result<()> {
        let page = self.load_page(db_path, page_num)?;

        if page.is_leaf() {
            // This is a leaf page - collect all its records
            for record in page.records() {
                records.push(record);
            }
        } else {
            // This is an interior page - traverse all child pages
            let child_pages = page.get_child_pages();
            for child_page_num in child_pages {
                // Validate child page number
                if child_page_num == 0 {
                    continue; // Skip invalid page numbers
                }
                self.traverse_btree(db_path, child_page_num as usize, records)?;
            }
        }

        Ok(())
    }

    // ---------------- Index search helpers ----------------

    /// Collect all rowids whose index key (country) equals `target_country`.
    /// `index_root_page` must point to the root of an index B-tree that stores
    /// (country TEXT, rowid INTEGER) records – exactly the schema of
    /// `idx_companies_country` used by the challenge.
    pub fn lookup_rowids_by_country(
        &self,
        db_path: &str,
        index_root_page: usize,
        target_country: &str,
    ) -> anyhow::Result<Vec<u64>> {
        let mut rowids = Vec::new();
        self.traverse_index(db_path, index_root_page, target_country, &mut rowids)?;
        Ok(rowids)
    }

    fn traverse_index(
        &self,
        db_path: &str,
        page_num: usize,
        target: &str,
        rowids: &mut Vec<u64>,
    ) -> anyhow::Result<()> {
        let page = self.load_page(db_path, page_num)?;

        match page.typ {
            PageType::IndexLeaf => {
                for (country, rowid) in page.index_leaf_entries() {
                    match country.as_str().cmp(target) {
                        std::cmp::Ordering::Less => continue, // still before our key
                        std::cmp::Ordering::Equal => rowids.push(rowid),
                        std::cmp::Ordering::Greater => break, // beyond target; no more matches in this leaf
                    }
                }
            }
            PageType::IndexInterior => {
                // Fetch interior entries and determine which child(ren) to explore.
                let entries = page.index_interior_entries();

                // We'll iterate to decide which sub-trees can possibly hold the target.
                for (i, (country_key, child_page)) in entries.iter().enumerate() {
                    use std::cmp::Ordering::*;
                    match target.cmp(country_key) {
                        Less => {
                            // Target lies entirely in left subtree (child_page)
                            self.traverse_index(db_path, *child_page as usize, target, rowids)?;
                            return Ok(());
                        }
                        Equal => {
                            // Traverse matching child
                            self.traverse_index(db_path, *child_page as usize, target, rowids)?;

                            // Also traverse the immediate right sibling subtree because duplicates
                            // could span boundaries.
                            if i + 1 < entries.len() {
                                let next_child = entries[i + 1].1;
                                self.traverse_index(db_path, next_child as usize, target, rowids)?;
                            } else if let Some(rightmost) = page.right_most_child {
                                self.traverse_index(db_path, rightmost as usize, target, rowids)?;
                            }
                            return Ok(());
                        }
                        Greater => {
                            // Keep scanning keys (*continue loop*)
                        }
                    }
                }

                // If we reach here, target > all keys – descend into rightmost child
                if let Some(rightmost) = page.right_most_child {
                    self.traverse_index(db_path, rightmost as usize, target, rowids)?;
                }
            }
            _ => anyhow::bail!("Unexpected page type in index traversal: {:?}", page.typ),
        }

        Ok(())
    }

    /// Fetch a single table record by rowid via B-tree navigation.
    pub fn fetch_record_by_rowid(
        &self,
        db_path: &str,
        table_root_page: usize,
        rowid: u64,
    ) -> anyhow::Result<Option<Record>> {
        self.search_table_btree(db_path, table_root_page, rowid)
    }

    fn search_table_btree(
        &self,
        db_path: &str,
        page_num: usize,
        target_rowid: u64,
    ) -> anyhow::Result<Option<Record>> {
        let page = self.load_page(db_path, page_num)?;

        match page.typ {
            PageType::TableLeaf => {
                for rec in page.records() {
                    if rec.id == target_rowid {
                        return Ok(Some(rec));
                    }
                }
                Ok(None)
            }
            PageType::TableInterior => {
                let entries = page.table_interior_entries();

                // iterate over entries to decide which child to descend
                for (i, (child_page, key_rowid)) in entries.iter().enumerate() {
                    if target_rowid < *key_rowid {
                        return self.search_table_btree(
                            db_path,
                            *child_page as usize,
                            target_rowid,
                        );
                    } else if target_rowid == *key_rowid {
                        // The row could be in left child or in the leaf page pointed by key? For table interior, exact key is not stored in child, row lives in left child.
                        return self.search_table_btree(
                            db_path,
                            *child_page as usize,
                            target_rowid,
                        );
                    }
                    // else continue loop
                }

                // If not found among keys, descend into rightmost child
                if let Some(rightmost) = page.right_most_child {
                    self.search_table_btree(db_path, rightmost as usize, target_rowid)
                } else {
                    Ok(None)
                }
            }
            _ => anyhow::bail!(
                "Unexpected page type while searching table btree: {:?}",
                page.typ
            ),
        }
    }

    /// Fetch multiple records by ascending rowids list, preserving order.
    pub fn fetch_records_by_rowids(
        &self,
        db_path: &str,
        table_root_page: usize,
        rowids: &[u64],
    ) -> anyhow::Result<Vec<Record>> {
        let mut results = Vec::with_capacity(rowids.len());
        for &rid in rowids {
            if let Some(rec) = self.fetch_record_by_rowid(db_path, table_root_page, rid)? {
                results.push(rec);
            }
        }
        Ok(results)
    }
}
