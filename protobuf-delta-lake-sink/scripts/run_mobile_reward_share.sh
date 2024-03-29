#!/bin/bash

  AWS_S3_ALLOW_UNSAFE_RENAME=true ../target/debug/protobuf-delta-lake-sink \
  --source-bucket iot-ingest \
  --source-region us-east-2 \
  --file-prefix mobile_reward_share \
  --source-proto-name "mobile_reward_share" \
  --source-proto-base-url https://raw.githubusercontent.com/helium/proto/master/src \
  --source-protos mapper.proto \
  --source-protos service/poc_mobile.proto \
  --target-bucket delta \
  --target-table bronze/mobile_reward_share \
  --target-region us-east-2 \
  --source-access-key-id minioadmin \
  --source-secret-access-key minioadmin \
  --target-access-key-id minioadmin \
  --target-secret-access-key minioadmin \
  --batch-size 1000000000 \
  --source-endpoint http://localhost:9000 \
  --target-endpoint http://localhost:9000 \
  --partition-timestamp-column start_period \
  --partition-timestamp-date-divisor 86400