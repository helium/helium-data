#!/bin/bash

AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
  --source-bucket iot-ingest \
  --source-region us-east-2 \
  --file-prefix iot_poc \
  --source-proto-name "lora_poc_v1" \
  --source-proto-base-url https://raw.githubusercontent.com/helium/proto/master/src \
  --source-protos data_rate.proto \
  --source-protos service/poc_lora.proto \
  --source-protos service/packet_verifier.proto \
  --target-bucket delta \
  --target-table bronze/iot_poc_v21 \
  --target-region us-east-2 \
  --source-access-key-id minioadmin \
  --source-secret-access-key minioadmin \
  --target-access-key-id minioadmin \
  --target-secret-access-key minioadmin \
  --batch-size 1000000000 \
  --source-endpoint http://localhost:9000 \
  --target-endpoint http://localhost:9000 \
  --partition-timestamp-column beacon_report.received_timestamp