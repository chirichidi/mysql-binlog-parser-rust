use crate::errors::EventParseError;
use std::io::{Read, Cursor};
use byteorder::{LittleEndian, ReadBytesExt};
use core::fmt;
use std::fmt::Debug;

// https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TypeCode {
    UnknownEvent,
    StartEventV3,
    QueryEvent,
    StopEvent,
    RotateEvent,
    IntvarEvent,
    LoadEvent,
    SlaveEvent,
    CreateFileEvent,
    AppendBlockEvent,
    ExecLoadEvent,
    DeleteFileEvent,
    NewLoadEvent,
    RandEvent,
    UserVarEvent,
    FormatDescriptionEvent,
    XidEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent,
    TableMapEvent,
    PreGaWriteRowsEvent,
    PreGaUpdateRowsEvent,
    PreGaDeleteRowsEvent,
    WriteRowsEventV1,
    UpdateRowsEventV1,
    DeleteRowsEventV1,
    IncidentEvent,
    HeartbeatLogEvent,
    IgnorableLogEvent,
    RowsQueryLogEvent,
    WriteRowsEventV2,
    UpdateRowsEventV2,
    DeleteRowsEventV2,
    GtidLogEvent,
    AnonymousGtidLogEvent,
    PreviousGtidsLogEvent,
}

impl TypeCode {
    fn from_byte(b: u8) -> Self {
        match b {
            0 => TypeCode::UnknownEvent,
            1 => TypeCode::StartEventV3,
            2 => TypeCode::QueryEvent,
            3 => TypeCode::StopEvent,
            4 => TypeCode::RotateEvent,
            5 => TypeCode::IntvarEvent,
            6 => TypeCode::LoadEvent,
            7 => TypeCode::SlaveEvent,
            8 => TypeCode::CreateFileEvent,
            9 => TypeCode::AppendBlockEvent,
            10 => TypeCode::ExecLoadEvent,
            11 => TypeCode::DeleteFileEvent,
            12 => TypeCode::NewLoadEvent,
            13 => TypeCode::RandEvent,
            14 => TypeCode::UserVarEvent,
            15 => TypeCode::FormatDescriptionEvent,
            16 => TypeCode::XidEvent,
            17 => TypeCode::BeginLoadQueryEvent,
            18 => TypeCode::ExecuteLoadQueryEvent,
            19 => TypeCode::TableMapEvent,
            20 => TypeCode::PreGaWriteRowsEvent,
            21 => TypeCode::PreGaUpdateRowsEvent,
            22 => TypeCode::PreGaDeleteRowsEvent,
            23 => TypeCode::WriteRowsEventV1,
            24 => TypeCode::UpdateRowsEventV1,
            25 => TypeCode::DeleteRowsEventV1,
            26 => TypeCode::IncidentEvent,
            27 => TypeCode::HeartbeatLogEvent,
            28 => TypeCode::IgnorableLogEvent,
            29 => TypeCode::RowsQueryLogEvent,
            30 => TypeCode::WriteRowsEventV2,
            31 => TypeCode::UpdateRowsEventV2,
            32 => TypeCode::DeleteRowsEventV2,
            33 => TypeCode::GtidLogEvent,
            34 => TypeCode::AnonymousGtidLogEvent,
            35 => TypeCode::PreviousGtidsLogEvent,
            _ => TypeCode::UnknownEvent
        }
    }
}

// https://dev.mysql.com/doc/internals/en/event-structure.html
pub struct Event {
    timestamp: u32,
    type_code: TypeCode,
    server_id: u32,
    pub event_length: u32,
    next_position: u32,
    flags: u16,
    data: Vec<u8>,
    offset: u64,
}

pub enum EventData {
    FormatDescriptionEvent {
        binlog_version: u16,
        server_version: String,
        create_timestamp: u32,
        common_header_len: u8
    },
}

impl Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Event {{ timestamp: {:?}, type_code: {:?}, server_id: {:?}, data_len: {:?}, offset: {:?}, flags: {:?}, event_length: {:?} }}",
               self.timestamp(),
               self.timestamp(),
               self.server_id,
               self.data.len(),
               self.offset(),
               self.flags(),
               self.event_length()
        )
    }
}

impl Event {
    pub fn parse<R: Read>(reader: &mut R, offset: u64) -> Result<Self, EventParseError> {

        // https://dev.mysql.com/doc/internals/en/binary-log-versions.html
        let mut event_header = [0u8; 19];
        match reader.read_exact(&mut event_header) {
            Ok(_) => {}
            Err(e) => { return Err(e.into()) }
        }

        let mut cursor = Cursor::new(event_header);
        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
        let type_code = TypeCode::from_byte(cursor.read_u8().unwrap());
        let server_id = cursor.read_u32::<LittleEndian>().unwrap();
        let event_length = cursor.read_u32::<LittleEndian>().unwrap();
        let next_position = cursor.read_u32::<LittleEndian>().unwrap();
        let flags = cursor.read_u16::<LittleEndian>().unwrap();
        let data_length: usize = (event_length - 19) as usize;

        let mut data = vec![0u8; data_length];
        reader.read_exact(&mut data).unwrap();

        Ok(Event {
            timestamp,
            type_code,
            server_id,
            event_length,
            next_position,
            flags,
            data,
            offset
        })
    }

    pub fn parse_event_data_by_type_code(type_code: TypeCode, data: &[u8]) -> Result<Option<EventData>, EventParseError> {

        let mut cursor = Cursor::new(data);

        // https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
        match type_code {
            TypeCode::FormatDescriptionEvent => {
                let binlog_version = cursor.read_u16::<LittleEndian>().unwrap();
                let server_version_beg = cursor.position() as usize;
                let server_version_end = server_version_beg
                    + data[server_version_beg..(server_version_beg + 50)]
                    .iter()
                    .position(|&u| u == 0)
                    .unwrap();
                let server_version = std::str::from_utf8(&data[server_version_beg..server_version_end]).unwrap().to_owned();
                cursor.set_position(cursor.position() + 50);
                let create_timestamp = cursor.read_u32::<LittleEndian>().unwrap();
                let common_header_len = cursor.read_u8().unwrap();
                let event_type_header_len = &data[cursor.position() as usize..];
                cursor.set_position(cursor.position() as u64 + event_type_header_len.len() as u64);

                Ok(Some(EventData::FormatDescriptionEvent {
                    binlog_version,
                    server_version,
                    create_timestamp,
                    common_header_len,
                }))
            }
            _ => { Ok(None) }
        }
    }

    pub fn type_code(&self) -> TypeCode {
        self.type_code
    }

    pub fn timestamp(&self) -> u32 {
        self.timestamp
    }

    pub fn next_position(&self) -> u64 {
        u64::from(self.next_position)
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn flags(&self) -> u16 {
        self.flags
    }

    pub fn event_length(&self) -> u32 {
        self.event_length
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}





#[cfg(test)]
mod tests {
    use crate::event::{Event, TypeCode};
    use std::fs::File;

    #[test]
    fn test_aa() {
        //given
        let path = "tests/asset/mysql-bin.100746";
        let mut reader = File::open(path).unwrap();

        //when
        let event = Event::parse(&mut reader, 0).unwrap();
        println!("event: {:?}", event);

        //then
        assert_eq!(event.type_code, TypeCode::FormatDescriptionEvent);
    }
}