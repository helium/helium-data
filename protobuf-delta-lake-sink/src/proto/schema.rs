use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use deltalake::{SchemaDataType, SchemaField, SchemaTypeArray, SchemaTypeStruct};
use futures::{stream, StreamExt};
use protobuf::reflect::{FileDescriptor, MessageDescriptor, RuntimeType};
use protobuf_parse::Parser;

async fn fetch_and_write_file(url: String) -> Result<PathBuf> {
    let response = reqwest::get(url.clone()).await?;
    let file_bytes = response.bytes().await?;
    let file_path =
        Path::new(".").join(Path::new(url.clone().split("/").last().context("Bad url")?)); // Set the desired file path

    let mut file = File::create(file_path.clone()).context("Failed to create file")?;
    file.write_all(&file_bytes)?;

    Ok(file_path)
}

// All urls containing the proto we want and deps
pub async fn get_descriptor(
    proto_urls: Vec<String>,
    proto_name: String,
) -> Result<MessageDescriptor> {
    let paths_raw = stream::iter(proto_urls)
        .map(|url| fetch_and_write_file(url))
        .buffer_unordered(2)
        .collect::<Vec<_>>()
        .await;

    let paths: Vec<PathBuf> = paths_raw.into_iter().collect::<Result<Vec<_>>>()?;

    let mut parser = Parser::new();
    parser.include(Path::new("."));
    for path in &paths {
        parser.input(&path);
    }
    let file_descriptor_set = parser
        .file_descriptor_set()
        .context("Failed to get descriptor set")?;
    let file_descriptors = FileDescriptor::new_dynamic_fds(file_descriptor_set.file, &[])
        .context("Failed to construct descriptor")?;

    file_descriptors
        .iter()
        .find_map(|f| f.message_by_package_relative_name(proto_name.as_str()))
        .context(format!("Message type not found {}", proto_name))
}

pub fn get_single_delta_schema(field_name: &str, field_type: RuntimeType) -> SchemaField {
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
                SchemaDataType::r#struct(SchemaTypeStruct::new(get_delta_schema(&m))),
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
}

pub fn get_delta_schema(descriptor: &MessageDescriptor) -> Vec<SchemaField> {
    descriptor
        .fields()
        .map(|f| {
            let field_name = f.name();
            let field_type = match f.runtime_field_type() {
                protobuf::reflect::RuntimeFieldType::Singular(t) => t,
                protobuf::reflect::RuntimeFieldType::Repeated(t) => {
                    return SchemaField::new(
                        field_name.to_string(),
                        SchemaDataType::array(SchemaTypeArray::new(
                            Box::new(get_single_delta_schema(field_name, t).get_type().clone()),
                            false, // Protobuf does not support nulls
                        )),
                        false, // Protobuf does not support nulls
                        HashMap::new(),
                    );
                }
                _ => panic!("Map fields are not supported"),
            };
            get_single_delta_schema(field_name, field_type)
        })
        .collect::<Vec<_>>()
}
