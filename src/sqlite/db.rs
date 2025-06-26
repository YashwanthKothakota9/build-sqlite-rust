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
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct Record {
    pub id: u64,
    pub values: Vec<RecordValue>,
}

#[derive(Debug)]
enum PageType {
    TableLeaf,
}

#[derive(Debug)]
pub struct Page {
    pub typ: PageType,
    pub cell_pointers: Vec<usize>,
    data: Vec<u8>,
}

impl Page {
    fn from_data(page_size: u16, data: Vec<u8>) -> Self {
        let typ = match data[0] {
            13 => PageType::TableLeaf,
            _ => panic!("Invalid page type: {}", data[0]),
        };
        let cell_count = u16::from_be_bytes([data[3], data[4]]);
        let data_offset = 8 + (cell_count as usize * 2);
        let offset = (page_size as usize) - data.len() + data_offset;
        let cell_pointers = data[8..data_offset]
            .chunks(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]) as usize - offset)
            .collect();
        let data = data[8 + (cell_count as usize * 2)..].to_vec();

        Self {
            typ,
            cell_pointers,
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

    fn get_be_bytes(n: usize, data: &mut impl Iterator<Item = u8>) -> [u8; 8] {
        let mut bytes = [0; 8];
        for i in 0..n {
            bytes[8 - n + i] = data.next().unwrap();
        }
        bytes
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
                    let value = i64::from_be_bytes(Self::get_be_bytes(n, &mut data));
                    RecordValue::Int(value)
                }
                7 => {
                    let value = f64::from_be_bytes(Self::get_be_bytes(8, &mut data));
                    RecordValue::Real(value)
                }
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
        let mut file = File::open(path)?;
        let page_offset = if page_number == 1 {
            DB_HEADER_SIZE
        } else {
            (page_number - 1) * (self.page_size as usize)
        };
        file.seek(SeekFrom::Start(page_offset as u64))?;
        let page_data_size = if page_number == 1 {
            self.page_size as usize - DB_HEADER_SIZE
        } else {
            self.page_size as usize
        };
        let mut page_data = vec![0; page_data_size];
        file.read_exact(&mut page_data)?;

        Ok(Page::from_data(self.page_size, page_data))
    }
}
