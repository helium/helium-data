#!/bin/bash

AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
  --source-bucket iot-ingest \
  --source-region us-east-2 \
  --file-prefix iot_beacon_ingest_report \
  --source-proto-name "lora_beacon_ingest_report_v1" \
  --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/data_rate.proto \
  --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto \
  --target-bucket delta \
  --target-table iot_beacon_ingest_report_v1 \
  --target-region us-east-2 \
  --source-access-key-id minioadmin \
  --source-secret-access-key minioadmin \
  --target-access-key-id minioadmin \
  --target-secret-access-key minioadmin \
  --source-endpoint http://localhost:9000 \
  --target-endpoint http://localhost:9000 \
  --partition-timestamp-column received_timestamp
