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
            _ => panic!("Invalid page type: {}", data[0]),
        };

        let right_most_child = match typ {
            PageType::TableInterior => {
                // Bytes 8-11 contain the rightmost child page number for interior pages
                Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]]))
            }
            PageType::TableLeaf => None,
        };

        let cell_count = u16::from_be_bytes([data[3], data[4]]);
        let data_offset = match typ {
            PageType::TableInterior => 12 + (cell_count as usize * 2), // Interior: header is 12 bytes
            PageType::TableLeaf => 8 + (cell_count as usize * 2),      // Leaf: header is 8 bytes
        };
        let offset = (page_size as usize) - data.len() + data_offset;

        let cell_pointer_start = match typ {
            PageType::TableInterior => 12, // Interior pages start cell pointers at byte 12
            PageType::TableLeaf => 8,      // Leaf pages start at byte 8
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

    fn get_record(&self, pointer: usize) -> Record {
        // TODO: Handle varints with size > 1 byte
        let mut offset = pointer;
        let size = Self::get_varint(&self.data, &mut offset) as usize;
        let id = Self::get_varint(&self.data, &mut offset) as u64;
        let header_size = Self::get_varint(&self.data, &mut offset) as usize;

        let header_boundary = offset + header_size - 1;
        let end_boundary = pointer + size + 2;
        let mut data = self.data[header_boundary..end_boundary].iter().copied();

        let mut values = Vec::with_capacity(header_size);
        let mut header_offset = 0;

        while header_offset < header_size - 1 {
            let typ = Self::get_varint(&self.data[offset..header_boundary], &mut header_offset);
            let value = match typ {
                0 => RecordValue::Null,
                1..=6 => {
                    let n = match typ {
                        5 => 6,
                        6 => 8,
                        v => v,
                    } as usize;
                    match Self::get_be_bytes(n, &mut data) {
                        Ok(bytes) => {
                            let value = i64::from_be_bytes(bytes);
                            RecordValue::Int(value)
                        }
                        Err(_) => {
                            return Record { id, values };
                        }
                    }
                }
                7 => match Self::get_be_bytes(8, &mut data) {
                    Ok(bytes) => {
                        let value = f64::from_be_bytes(bytes);
                        RecordValue::Real(value)
                    }
                    Err(_) => {
                        return Record { id, values };
                    }
                },
                8 => RecordValue::Int(0),
                9 => RecordValue::Int(1),
                _ if typ >= 12 => {
                    let length = ((typ - 12) / 2) as usize;
                    let value = (&mut data).take(length).collect::<Vec<u8>>();
                    if typ % 2 == 0 {
                        RecordValue::Blob(value)
                    } else {
                        let value = String::from_utf8_lossy(&value).to_string();
                        RecordValue::Text(value)
                    }
                }
                _ => panic!("Invalid value type: {}", typ),
            };
            values.push(value);
        }

        Record { id, values }
    }

    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        self.cell_pointers.iter().map(|i| self.get_record(*i))
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self.typ, PageType::TableLeaf)
    }

    pub fn get_child_pages(&self) -> Vec<u32> {
        match self.typ {
            PageType::TableLeaf => Vec::new(), // Leaf pages have no children
            PageType::TableInterior => {
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
        }
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
}
