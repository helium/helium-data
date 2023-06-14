use anyhow::{bail, Context, Result};
use deltalake::arrow::array::StringArray;
use deltalake::arrow::datatypes::{Field, Fields};
use deltalake::{
    arrow::{
        array::{
            Array, ArrayBuilder, ArrayData, ArrayRef, BinaryBuilder, BooleanBuilder, BufferBuilder,
            Date32Array, Decimal128Array, GenericListArray, PrimitiveBuilder, StringBuilder,
            StructArray,
        },
        datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int32Type, Int64Type},
        record_batch::RecordBatch,
    },
    Schema, SchemaTypeStruct,
};
use protobuf::reflect::{EnumDescriptor, ReflectValueBox};
use protobuf::{
    reflect::{FieldDescriptor, MessageDescriptor, ReflectValueRef, RuntimeType},
    MessageDyn,
};
use std::{any::Any, sync::Arc};

use super::get_delta_schema;

trait ReflectBuilder: ArrayBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>);
}

macro_rules! make_builder_wrapper {
    ($name:ident, $ty:ty) => {
        struct $name {
            pub builder: $ty,
        }

        impl ArrayBuilder for $name {
            fn len(&self) -> usize {
                self.builder.len()
            }

            fn is_empty(&self) -> bool {
                self.builder.is_empty()
            }

            fn finish(&mut self) -> ArrayRef {
                Arc::new(self.builder.finish())
            }

            fn finish_cloned(&self) -> ArrayRef {
                Arc::new(self.builder.finish_cloned())
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }

            fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
                self
            }
        }
    };
}

make_builder_wrapper!(BoolReflectBuilder, BooleanBuilder);
make_builder_wrapper!(BinaryReflectBuilder, BinaryBuilder);
make_builder_wrapper!(StringReflectBuilder, StringBuilder);

impl ReflectBuilder for BinaryReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| i.to_bytes().expect("Not bytes").to_vec())
                .unwrap_or_default(),
        )
    }
}

impl ReflectBuilder for StringReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| {
                i.to_str()
                    .expect(format!("Not a string {:?}", i).as_str())
                    .to_string()
            })
            .unwrap_or_default(),
        )
    }
}

impl ReflectBuilder for BoolReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| i.to_bool().expect("Not a boolean"))
                .unwrap_or_default(),
        )
    }
}

pub struct EnumReflectBuilder {
    pub builder: StringBuilder,
    pub enum_descriptor: EnumDescriptor,
}

impl ArrayBuilder for EnumReflectBuilder {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.builder.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl ReflectBuilder for EnumReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| {
                self.enum_descriptor
                    .value_by_number(i.to_enum_value().expect("Not an enum"))
                    .expect("Value not found")
                    .full_name()
            })
            .unwrap_or_default(),
        )
    }
}

struct PrimitiveReflectBuilder<T: ArrowPrimitiveType> {
    pub builder: PrimitiveBuilder<T>,
}

impl<T: ArrowPrimitiveType> ArrayBuilder for PrimitiveReflectBuilder<T> {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.builder.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Int32Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| i.to_i32().expect("Not an i32"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Int64Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| {
                i.to_i64()
                    .unwrap_or_else(|| i64::from(i.to_u32().expect("Not an i64 or u32")))
            })
            .unwrap_or_default(),
        );
    }
}

pub struct U64ReflectBuilder {
    pub values: Vec<u64>,
}

impl ArrayBuilder for U64ReflectBuilder {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(
                self.values
                    .iter()
                    .map(|v| i128::from(*v))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(23, 0)
            .expect("Failed to add precision"),
        )
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(
                self.values
                    .iter()
                    .map(|v| i128::from(*v))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(23, 0)
            .expect("Failed to add precision"),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl ReflectBuilder for U64ReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.values.push(
            v.map(|i| i.to_u64().expect("Not a u64"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Float32Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| i.to_f32().expect("Not a f32"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Float64Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        self.builder.append_value(
            v.map(|i| i.to_f64().expect("Not a f64"))
                .unwrap_or_default(),
        );
    }
}

struct StructReflectBuilder {
    pub fields: Fields,
    pub builders: Vec<Box<dyn ReflectArrayBuilder>>,
    pub descriptor: MessageDescriptor,
}

impl ArrayBuilder for StructReflectBuilder {
    fn len(&self) -> usize {
        self.builders.get(0).unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&mut self) -> ArrayRef {
        self.validate_content();
        let length = self.len();

        let mut child_data = Vec::with_capacity(self.builders.len());
        for f in &mut self.builders {
            let arr = f.finish();
            child_data.push(arr.to_data());
        }

        let builder = ArrayData::builder(DataType::Struct(self.fields.clone()))
            .len(length)
            .child_data(child_data)
            .null_bit_buffer(None);

        let array_data = unsafe { builder.build_unchecked() };
        Arc::new(StructArray::from(array_data))
    }

    fn finish_cloned(&self) -> ArrayRef {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StructReflectBuilder {
    /// Constructs and validates contents in the builder to ensure that
    /// - fields and field_builders are of equal length
    /// - the number of items in individual field_builders are equal to self.len()
    fn validate_content(&self) {
        if !self.builders.iter().all(|x| x.len() == self.len()) {
            panic!("StructBuilder and field_builders are of unequal lengths.");
        }
    }
}

impl ReflectBuilder for StructReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) {
        let message_ref = v
            .map(|i| i.to_message().expect("Not a message"))
            .expect("Messages can't be none");
        let message = &*message_ref;
        for (index, field) in self.descriptor.fields().enumerate() {
            match field.runtime_field_type() {
                protobuf::reflect::RuntimeFieldType::Singular(_) => {
                    let builder = self.builders.get_mut(index).unwrap();
                    builder.append_value(field.get_singular(message))
                }
                protobuf::reflect::RuntimeFieldType::Repeated(_) => {
                    panic!("Repeated fields in a nested message are not supported")
                }
                protobuf::reflect::RuntimeFieldType::Map(_, _) => {
                    panic!("Map fields are not supported")
                }
            };
        }
    }
}

fn runtime_type_to_data_type(value: &RuntimeType) -> DataType {
    match value {
        RuntimeType::I32 => DataType::Int32,
        RuntimeType::I64 => DataType::Int64,
        RuntimeType::U32 => DataType::UInt32,
        RuntimeType::U64 => DataType::UInt64,
        RuntimeType::F32 => DataType::Float32,
        RuntimeType::F64 => DataType::Float64,
        RuntimeType::Bool => DataType::Boolean,
        RuntimeType::String => DataType::Binary,
        RuntimeType::VecU8 => DataType::Binary,
        RuntimeType::Enum(_) => DataType::Binary,
        RuntimeType::Message(m) => {
            let fields = get_delta_schema(m);
            let schema = <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(
                &SchemaTypeStruct::new(fields),
            )
            .expect("Failed to get schema");
            DataType::Struct(schema.fields)
        }
    }
}

trait ReflectArrayBuilder: ReflectBuilder + ArrayBuilder {}

impl<T: ArrayBuilder + ReflectBuilder + ?Sized> ReflectArrayBuilder for T {}

fn get_builder(t: &RuntimeType, capacity: usize) -> Result<Box<dyn ReflectArrayBuilder>> {
    Ok(match t {
        RuntimeType::I32 => Box::new(PrimitiveReflectBuilder::<Int32Type> {
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        RuntimeType::I64 => Box::new(PrimitiveReflectBuilder {
            builder: PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
        }),
        RuntimeType::U32 => Box::new(PrimitiveReflectBuilder {
            builder: PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
        }),
        RuntimeType::U64 => Box::new(U64ReflectBuilder { values: vec![] }),
        RuntimeType::F32 => Box::new(PrimitiveReflectBuilder {
            builder: PrimitiveBuilder::<Float32Type>::with_capacity(capacity),
        }),
        RuntimeType::F64 => Box::new(PrimitiveReflectBuilder {
            builder: PrimitiveBuilder::<Float64Type>::with_capacity(capacity),
        }),
        RuntimeType::Bool => Box::new(BoolReflectBuilder {
            builder: BooleanBuilder::with_capacity(capacity),
        }),
        RuntimeType::String => Box::new(StringReflectBuilder {
            builder: StringBuilder::with_capacity(capacity, 1024),
        }),
        RuntimeType::VecU8 => Box::new(BinaryReflectBuilder {
            builder: BinaryBuilder::with_capacity(capacity, 1024),
        }),
        RuntimeType::Enum(enum_descriptor) => Box::new(EnumReflectBuilder {
            builder: StringBuilder::with_capacity(capacity, 1024),
            enum_descriptor: enum_descriptor.clone(),
        }),
        RuntimeType::Message(m) => {
            let schema = Schema::new(get_delta_schema(m));
            let arrow_schema =
                <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(&schema)?;
            let builders = m
                .clone()
                .fields()
                .map(|field| match field.runtime_field_type() {
                    protobuf::reflect::RuntimeFieldType::Singular(t) => get_builder(&t, capacity),
                    protobuf::reflect::RuntimeFieldType::Repeated(_) => {
                        bail!("Repeated fields in a nested message are not supported")
                    }
                    protobuf::reflect::RuntimeFieldType::Map(_, _) => {
                        bail!("Map fields are not supported")
                    }
                })
                .collect::<Result<Vec<Box<dyn ReflectArrayBuilder>>>>()?;
            Box::new(StructReflectBuilder {
                fields: arrow_schema.fields,
                builders: builders,
                descriptor: m.clone(),
            })
        }
    })
}

fn field_to_arrow(f: &FieldDescriptor, messages: &Vec<&dyn MessageDyn>) -> Result<Arc<dyn Array>> {
    Ok(match f.runtime_field_type() {
        protobuf::reflect::RuntimeFieldType::Singular(t) => {
            let mut builder: Box<dyn ReflectArrayBuilder> = get_builder(&t, messages.len())?;

            for message in messages.iter() {
                builder.append_value(f.get_singular(*message));
            }

            Arc::new(builder.finish())
        }
        protobuf::reflect::RuntimeFieldType::Repeated(t) => {
            let mut builder: Box<dyn ReflectArrayBuilder> = get_builder(&t, messages.len())?;
            let mut offsets = BufferBuilder::<i32>::new(messages.len() + 1);

            offsets.append(0);
            for message in messages.iter() {
                let repeated = f.get_repeated(*message);
                for i in 0..repeated.len() {
                    let value = repeated.get(i);
                    builder.append_value(Some(value));
                }
                offsets.append(i32::try_from(builder.len()).unwrap());
            }
            let field = Arc::new(Field::new("item", runtime_type_to_data_type(&t), false));
            let data_type = DataType::List(field);
            let values_arr = builder.finish();
            let values_data = values_arr.to_data();
            let array_data_builder = ArrayData::builder(data_type)
                .len(messages.len())
                .add_buffer(offsets.finish())
                .add_child_data(values_data)
                .null_bit_buffer(None);
            let array_data = unsafe { array_data_builder.build_unchecked() };
            Arc::new(GenericListArray::<i32>::from(array_data))
        }
        protobuf::reflect::RuntimeFieldType::Map(_, _) => panic!("Map fields are not supported"),
    })
}

pub fn to_record_batch(
    delta_schema: &Schema,
    descriptor: MessageDescriptor,
    // Tuple of file name and message
    messages_with_files: Vec<(&str, &dyn MessageDyn)>,
    partition_timestamp_column: Option<String>,
) -> Result<RecordBatch> {
    let messages = messages_with_files.iter().map(|m| m.1).collect::<Vec<_>>();
    let files = messages_with_files.iter().map(|m| m.0).collect::<Vec<_>>();
    let fields = descriptor.fields();
    let arrow_schema =
        <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(delta_schema)?;
    let mut arrow = fields
        .map(|f| field_to_arrow(&f, &messages))
        .collect::<Result<Vec<_>>>()?;

    if let Some(partition_timestamp_column) = partition_timestamp_column {
        let field_refs: Vec<Result<ReflectValueBox>> = get_fields(
            &descriptor,
            messages,
            &partition_timestamp_column.split(".").collect::<Vec<_>>(),
        );
        let timestamps = field_refs.into_iter().map(|field| {
            field
                .context("Timestamp not found")?
                .as_value_ref()
                .to_u64()
                .context("Not a u64")
        });
        let dates = timestamps
            .map(|t| i32::try_from(t? / (1000 * 60 * 60 * 24)).context("Not an int32"))
            .collect::<Result<Vec<i32>>>()?;
        let date_data: Arc<dyn Array> = Arc::new(Date32Array::from(dates));

        arrow.push(date_data);

        let file_data: Arc<dyn Array> = Arc::new(StringArray::from(files));
        arrow.push(file_data);
    }
    RecordBatch::try_new(Arc::new(arrow_schema), arrow).context("Failed to create record batch")
}

/// Recursively get field at path
fn get_field(
    descriptor: &MessageDescriptor,
    message: &dyn MessageDyn,
    path: &[&str],
) -> Result<ReflectValueBox> {
    let field_descriptor = descriptor
        .field_by_name(path[0])
        .context("Field not found")?;
    let value: Option<ReflectValueBox> = field_descriptor.get_singular(message).map(|i| i.to_box());
    if path.len() != 1 {
        if let Some(val) = value {
            match field_descriptor.runtime_field_type() {
                protobuf::reflect::RuntimeFieldType::Singular(m) => match m {
                    RuntimeType::Message(m) => {
                        let sub_msg = val.as_value_ref().to_message().context("Not a message")?;
                        return get_field(&m, &*sub_msg, &path[1..].to_vec());
                    }
                    _ => bail!("Field is not a message"),
                },
                _ => bail!("Cannot get field by path from a non-singular field"),
            }
        }
    }

    value.context("No value at path")
}

fn get_fields<'a>(
    descriptor: &MessageDescriptor,
    messages: Vec<&'a dyn MessageDyn>,
    path: &[&str],
) -> Vec<Result<ReflectValueBox>> {
    messages
        .iter()
        .map(|m| get_field(descriptor, *m, path))
        .collect::<Vec<_>>()
}
