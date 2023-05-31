use std::ops::Deref;
use std::{any::Any, sync::Arc};

use deltalake::arrow::datatypes::Fields;
use deltalake::{
    arrow::{
        array::{
            Array, ArrayBuilder, ArrayData, ArrayRef, BinaryBuilder, BooleanBuilder, BufferBuilder,
            Date32Array, Decimal128Array, GenericListArray, ListBuilder, PrimitiveBuilder,
            StringBuilder, StructArray,
        },
        datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int32Type, Int64Type},
        record_batch::RecordBatch,
    },
    Schema, SchemaTypeStruct,
};
use protobuf::reflect::EnumDescriptor;
use protobuf::{
    reflect::{FieldDescriptor, MessageDescriptor, ReflectValueRef, RuntimeType},
    MessageDyn,
};

use super::get_delta_schema;

trait ReflectBuilder: ArrayBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> ();
}

trait ReflectListBuilder {
    fn append(&mut self, is_valid: bool);
    fn finish(&mut self) -> GenericListArray<i32>;
}

struct ReflectListBuilderWrapper<T: ArrayBuilder> {
    pub builder: ListBuilder<T>,
}

impl<T: ArrayBuilder> ReflectListBuilder for ReflectListBuilderWrapper<T> {
    /// Finish the current variable-length list array slot
    ///
    /// # Panics
    ///
    /// Panics if the length of [`Self::values`] exceeds `OffsetSize::MAX`
    fn append(&mut self, is_valid: bool) {
        self.builder.append(is_valid)
    }

    fn finish(&mut self) -> GenericListArray<i32> {
        self.builder.finish()
    }
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
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
        self.builder.append_value(
            v.map(|i| i.to_bytes().expect("Not bytes").to_vec())
                .unwrap_or_default(),
        )
    }
}

impl ReflectBuilder for StringReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
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
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
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
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
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
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
        self.builder.append_value(
            v.map(|i| i.to_i32().expect("Not an i32"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Int64Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
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
        todo!()
    }
}

impl ReflectBuilder for U64ReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
        self.values.push(
            v.map(|i| i.to_u64().expect("Not a u64"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Float32Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
        self.builder.append_value(
            v.map(|i| i.to_f32().expect("Not a f32"))
                .unwrap_or_default(),
        );
    }
}

impl ReflectBuilder for PrimitiveReflectBuilder<Float64Type> {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
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
        todo!()
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

// impl ReflectArrayBuilder for StructReflectBuilder {}

impl ReflectBuilder for StructReflectBuilder {
    fn append_value(&mut self, v: Option<ReflectValueRef>) -> () {
        let message_ref = v
            .map(|i| i.to_message().expect("Not a message"))
            .expect("Messages can't be none");
        let message = message_ref.deref();
        for (index, field) in self.descriptor.fields().enumerate() {
            match field.runtime_field_type() {
                protobuf::reflect::RuntimeFieldType::Singular(_) => {
                    let builder = self.builders.get_mut(index).unwrap();
                    builder.append_value(field.get_singular(message))
                }
                protobuf::reflect::RuntimeFieldType::Repeated(_) => todo!(),
                protobuf::reflect::RuntimeFieldType::Map(_, _) => todo!(),
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
            let fields = get_delta_schema(m, false);
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

fn get_builder(t: &RuntimeType, capacity: usize) -> Box<dyn ReflectArrayBuilder> {
    match t {
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
            let schema = Schema::new(get_delta_schema(m, false));
            let arrow_schema =
                <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(&schema)
                    .expect("Failed to get arrow schema from delta schema");
            let builders = m
                .clone()
                .fields()
                .map(|field| match field.runtime_field_type() {
                    protobuf::reflect::RuntimeFieldType::Singular(t) => get_builder(&t, capacity),
                    protobuf::reflect::RuntimeFieldType::Repeated(_) => todo!(),
                    protobuf::reflect::RuntimeFieldType::Map(_, _) => todo!(),
                })
                .collect::<Vec<Box<dyn ReflectArrayBuilder>>>();
            Box::new(StructReflectBuilder {
                fields: arrow_schema.fields,
                builders: builders,
                descriptor: m.clone(),
            })
        }
    }
}

fn field_to_arrow(f: &FieldDescriptor, messages: &Vec<&dyn MessageDyn>) -> Arc<dyn Array> {
    match f.runtime_field_type() {
        protobuf::reflect::RuntimeFieldType::Singular(t) => {
            let mut builder: Box<dyn ReflectBuilder> = get_builder(&t, messages.len());

            for message in messages.iter() {
                builder.append_value(f.get_singular(*message));
            }

            Arc::new(builder.finish())
        }
        protobuf::reflect::RuntimeFieldType::Repeated(t) => {
            let mut builder: Box<dyn ReflectBuilder> = get_builder(&t, messages.len());
            let mut offsets = BufferBuilder::<i32>::new(messages.len() + 1);

            for message in messages.iter() {
                let repeated = f.get_repeated(*message);
                for i in 0..repeated.len() {
                    let value = repeated.get(i);
                    offsets.append(i32::try_from(builder.len()).unwrap());
                    builder.append_value(Some(value));
                }
            }
            let data_type = runtime_type_to_data_type(&t);
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
        protobuf::reflect::RuntimeFieldType::Map(_, _) => todo!(),
    }
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
