use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::Write,
    path::{self, Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use chrono::{Datelike, NaiveDateTime};
use deltalake::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
            Float64Array, Int32Array, Int64Array, StringArray, StructArray, UInt64Array,
        },
        datatypes::Field,
        record_batch::RecordBatch,
    },
    parquet::{arrow, data_type::Decimal},
    Schema, SchemaDataType, SchemaField, SchemaTypeStruct,
};
use futures::{stream, StreamExt};
use lazy_static::__Deref;
use protobuf::{
    descriptor::{FileDescriptorProto, FileDescriptorSet},
    reflect::{FieldDescriptor, FileDescriptor, MessageDescriptor},
    Message, MessageDyn,
};
use protobuf_parse::Parser;

async fn fetch_and_write_file(url: String) -> Result<PathBuf, reqwest::Error> {
    let response = reqwest::get(url.clone()).await?;
    let file_bytes = response.bytes().await?;
    let file_path = Path::new(".").join(Path::new(url.clone().split("/").last().unwrap())); // Set the desired file path

    let mut file = File::create(file_path.clone()).unwrap();
    file.write_all(&file_bytes).unwrap();

    Ok(file_path)
}
// All urls containing the proto we want and deps
pub async fn get_descriptor(proto_urls: Vec<String>, proto_name: String) -> MessageDescriptor {
    let paths_raw = stream::iter(proto_urls)
        .map(|url| fetch_and_write_file(url))
        .buffer_unordered(2)
        .collect::<Vec<_>>()
        .await;

    let paths: Vec<PathBuf> = paths_raw
        .into_iter()
        .map(|r| r.expect("Reqwest failed"))
        .collect();

    let mut parser = Parser::new();
    parser.include(Path::new("."));
    for path in &paths {
        parser.input(&path);
    }
    let file_descriptor_set = parser
        .file_descriptor_set()
        .expect("Failed to get descriptor set");
    let file_descriptors = FileDescriptor::new_dynamic_fds(file_descriptor_set.file, &[])
        .expect("Failed to construct descriptor");

    file_descriptors
        .iter()
        .find_map(|f| f.message_by_package_relative_name(proto_name.as_str()))
        .expect("Message type not found")
}

pub fn get_delta_schema(descriptor: MessageDescriptor, append_date: bool) -> Vec<SchemaField> {
    let mut ret = descriptor
        .fields()
        .map(|f| {
            let field_type = match f.runtime_field_type() {
                protobuf::reflect::RuntimeFieldType::Singular(t) => t,
                _ => panic!("Only singular fields are supported"),
            };
            let field_name = f.name();
            let field_type = match field_type {
                protobuf::reflect::RuntimeType::I32 => "integer",
                protobuf::reflect::RuntimeType::I64 => "long",
                protobuf::reflect::RuntimeType::U32 => "long",
                protobuf::reflect::RuntimeType::U64 => "decimal(23,0)",
                protobuf::reflect::RuntimeType::F32 => "float",
                protobuf::reflect::RuntimeType::F64 => "double",
                protobuf::reflect::RuntimeType::Bool => "boolean",
                protobuf::reflect::RuntimeType::String => "string",
                protobuf::reflect::RuntimeType::VecU8 => "binary",
                protobuf::reflect::RuntimeType::Enum(_) => "string",
                protobuf::reflect::RuntimeType::Message(m) => {
                    return SchemaField::new(
                        field_name.to_string(),
                        SchemaDataType::r#struct(SchemaTypeStruct::new(get_delta_schema(m, false))),
                        false, // Protobuf does not support nulls
                        HashMap::new(),
                    );
                }
            };

            SchemaField::new(
                field_name.to_string(),
                SchemaDataType::primitive(field_type.to_string()),
                false, // Protobuf does not support nulls
                HashMap::new(),
            )
        })
        .collect::<Vec<_>>();

    if append_date {
        let date_field = SchemaField::new(
            "date".to_string(),
            SchemaDataType::primitive("date".to_string()),
            false,
            HashMap::new(),
        );
        ret.push(date_field);
    }

    ret
}

fn field_to_arrow(f: &FieldDescriptor, messages: &Vec<&dyn MessageDyn>) -> Arc<dyn Array> {
    let field_type = match f.runtime_field_type() {
        protobuf::reflect::RuntimeFieldType::Singular(t) => t,
        _ => panic!("Only singular fields are supported"),
    };

    let ret: Arc<dyn Array> = match field_type {
        protobuf::reflect::RuntimeType::Message(m) => {
            let values = messages
                .iter()
                .map(|message| f.get_message(*message))
                .collect::<Vec<_>>();

            let values_refs: Vec<&dyn MessageDyn> = values.iter().map(|arc| arc.deref()).collect();

            Arc::new(
                StructArray::try_from(
                    m.fields()
                        .collect::<Vec<_>>()
                        .iter()
                        .map(|sub_field| {
                            let result = field_to_arrow(&sub_field, &values_refs);
                            let name = sub_field.name();
                            (name, result.into())
                        })
                        .collect::<Vec<(&str, ArrayRef)>>(),
                )
                .expect("Failed to create struct array"),
            )
        }
        _ => {
            let values = messages.iter().map(|message| f.get_singular(*message));
            match field_type {
                protobuf::reflect::RuntimeType::I32 => {
                    let v: Vec<i32> = values
                        .map(|v| {
                            v.map(|i| i.to_i32().expect("Not an i32"))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(Int32Array::from(v))
                }
                protobuf::reflect::RuntimeType::I64 => {
                    let v: Vec<i64> = values
                        .map(|v| {
                            v.map(|i| i.to_i64().expect(format!("Not an i64 {:?}", i).as_str()))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(Int64Array::from(v))
                }
                protobuf::reflect::RuntimeType::U32 => {
                    let v: Vec<i64> = values
                        .map(|v| {
                            v.map(|i| {
                                i64::from(i.to_u32().expect(format!("Not an i64 {:?}", i).as_str()))
                            })
                            .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(Int64Array::from(v))
                }
                protobuf::reflect::RuntimeType::U64 => {
                    let v: Vec<i128> = values
                        .map(|v| {
                            v.map(|i| i128::from(i.to_u64().expect("Not a u64")))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(
                        Decimal128Array::from(v)
                            .with_precision_and_scale(23, 0)
                            .expect("Failed to add precision"),
                    )
                }
                protobuf::reflect::RuntimeType::F32 => {
                    let v: Vec<f32> = values
                        .map(|v| {
                            v.map(|i| i.to_f32().expect("Not an f32"))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(Float32Array::from(v))
                }
                protobuf::reflect::RuntimeType::F64 => {
                    let v: Vec<f64> = values
                        .map(|v| {
                            v.map(|i| i.to_f64().expect("Not an f64"))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(Float64Array::from(v))
                }
                protobuf::reflect::RuntimeType::Bool => {
                    let v: Vec<bool> = values
                        .map(|v| {
                            v.map(|i| i.to_bool().expect("Not a bool"))
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(BooleanArray::from(v))
                }
                protobuf::reflect::RuntimeType::String => {
                    let v: Vec<String> = values
                        .map(|v| {
                            v.map(|i| i.to_str().expect("Not a string").to_string())
                                .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(StringArray::from(v))
                }
                protobuf::reflect::RuntimeType::VecU8 => {
                    let v: Vec<Vec<u8>> = values
                        .map(|v| {
                            v.map(|i| i.to_bytes().expect("Not bytes").to_vec())
                                .unwrap_or_default()
                        })
                        .collect();
                    let cloned_v: Vec<&[u8]> = v.iter().map(|v| v.as_slice()).collect();
                    Arc::new(BinaryArray::from(cloned_v))
                }
                protobuf::reflect::RuntimeType::Enum(e) => {
                    let v: Vec<String> = values
                        .map(|v| {
                            v.map(|i| {
                                e.value_by_number(i.to_enum_value().expect("Not an enum"))
                                    .expect("Value not found")
                                    .full_name()
                            })
                            .unwrap_or_default()
                        })
                        .collect();
                    Arc::new(StringArray::from(v))
                }
                protobuf::reflect::RuntimeType::Message(_) => panic!("This shouldn't happen"),
            }
        }
    };

    ret
}

pub fn to_record_batch(
    delta_schema: &Schema,
    descriptor: MessageDescriptor,
    messages: Vec<&dyn MessageDyn>,
    partition_timestamp_column: Option<String>,
) -> RecordBatch {
    let fields = descriptor.fields();
    let arrow_schema =
        <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(delta_schema)
            .expect("Failed to get arrow schema from delta schema");
    println!("Batching {} messages", messages.len());
    let mut arrow = fields
        .map(|f| field_to_arrow(&f, &messages))
        .collect::<Vec<_>>();

    if let Some(partition_timestamp_column) = partition_timestamp_column {
        let timestamp_field = descriptor
            .field_by_name(partition_timestamp_column.as_str())
            .expect(format!("No timestamp field {}", partition_timestamp_column).as_str());
        let timestamps = messages.iter().map(|m| {
            timestamp_field
                .get_singular(*m)
                .expect("No timestamp")
                .to_u64()
                .expect("Not a u64")
        });
        let date_data: Arc<dyn Array> = Arc::new(Date32Array::from(
            timestamps
                .map(|t| i32::try_from(t / (1000 * 60 * 60 * 24)).expect("Not a valid i32"))
                .collect::<Vec<i32>>(),
        ));

        arrow.push(date_data);
    }
    RecordBatch::try_new(Arc::new(arrow_schema), arrow).expect("Failed to create record batch")
}
