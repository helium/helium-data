# Specify the base image
FROM rust:1.68.2 as builder

RUN apt-get update && apt-get install -y protobuf-compiler 

# Set the working directory
WORKDIR /app

# Copy the Cargo files and install dependencies
COPY Cargo.toml ./
# Copy the source code and build the application
COPY src ./src
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y protobuf-compiler ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/protobuf-delta-lake-sink /app/protobuf-delta-lake-sink

# Start the application
ENTRYPOINT ["/app/protobuf-delta-lake-sink"]
