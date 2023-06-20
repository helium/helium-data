#!/bin/bash

# AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
#   --source-bucket iot-ingest \
#   --source-region us-east-2 \
#   --file-prefix iot_beacon_ingest_report \
#   --source-proto-name "lora_beacon_ingest_report_v1" \
#   --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/data_rate.proto \
#   --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto \
#   --target-bucket delta \
#   --target-table iot_beacon_ingest_report_v1 \
#   --target-region us-east-2 \
#   --source-access-key-id minioadmin \
#   --source-secret-access-key minioadmin \
#   --target-access-key-id minioadmin \
#   --target-secret-access-key minioadmin \
#   --source-endpoint http://localhost:9000 \
#   --target-endpoint http://localhost:9000 \
#   --partition-timestamp-column received_timestamp


# AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
#   --source-bucket iot-ingest \
#   --source-region us-east-2 \
#   --file-prefix iot_poc \
#   --source-proto-name "lora_poc_v1" \
#   --source-proto-base-url https://raw.githubusercontent.com/helium/proto/master/src \
#   --source-protos data_rate.proto \
#   --source-protos service/poc_lora.proto \
#   --source-protos service/packet_verifier.proto \
#   --target-bucket delta \
#   --target-table bronze/iot_poc_v17 \
#   --target-region us-east-2 \
#   --source-access-key-id minioadmin \
#   --source-secret-access-key minioadmin \
#   --target-access-key-id minioadmin \
#   --target-secret-access-key minioadmin \
#   --batch-size 1000000000 \
#   --source-endpoint http://localhost:9000 \
#   --target-endpoint http://localhost:9000 \
#   --partition-timestamp-column beacon_report.received_timestamp

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
  --target-table bronze/packet_router_packet_report_v1 \
  --target-region us-east-2 \
  --source-access-key-id minioadmin \
  --source-secret-access-key minioadmin \
  --target-access-key-id minioadmin \
  --target-secret-access-key minioadmin \
  --batch-size 1000000000 \
  --source-endpoint http://localhost:9000 \
  --target-endpoint http://localhost:9000 \
  --partition-timestamp-column gateway_tmst


# docker run -it -e AWS_S3_ALLOW_UNSAFE_RENAME=true docker.io/library/protobuf-delta-lake-sink:latest \
#   --source-bucket iot-ingest \
#   --source-region us-east-2 \
#   --file-prefix iot_poc \
#   --source-proto-name "lora_poc_v1" \
#   --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/data_rate.proto \
#   --source-protos https://raw.githubusercontent.com/helium/proto/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto \
#   --target-bucket delta \
#   --target-table iot_poc_v1 \
#   --target-region us-east-2 \
#   --source-access-key-id minioadmin \
#   --source-secret-access-key minioadmin \
#   --target-access-key-id minioadmin \
#   --target-secret-access-key minioadmin \
#   --source-endpoint http://host.docker.internal:9000 \
#   --target-endpoint http://host.docker.internal:9000 \
#   --partition-timestamp-column beacon_report.received_timestamp

