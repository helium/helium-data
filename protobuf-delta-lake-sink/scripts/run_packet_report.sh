#!/bin/bash

AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
  --source-bucket iot-ingest \
  --source-region us-east-2 \
  --file-prefix packetreport \
  --source-proto-name "packet_router_packet_report_v1" \
  --source-proto-base-url https://raw.githubusercontent.com/helium/proto/master/src \
  --source-protos data_rate.proto \
  --source-protos region.proto \
  --source-protos service/packet_router.proto \
  --target-bucket delta \
  --target-table bronze/packet_router_packet_report_v7 \
  --target-region us-east-2 \
  --source-access-key-id minioadmin \
  --source-secret-access-key minioadmin \
  --target-access-key-id minioadmin \
  --target-secret-access-key minioadmin \
  --batch-size 1000000000 \
  --source-endpoint http://localhost:9000 \
  --target-endpoint http://localhost:9000 \
  --partition-timestamp-column received_timestamp

