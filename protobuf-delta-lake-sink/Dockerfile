# Specify the base image
FROM rust:1.75.0 as builder

RUN apt-get update && apt-get install -y protobuf-compiler 

# Set the working directory
WORKDIR /app

# Copy the Cargo files and install dependencies
COPY Cargo.toml ./
COPY Cargo.lock ./

# Do a build before we add the source to cache the compiled deps
RUN mkdir ./src && echo 'fn main() { println!("Dummy!"); }' > ./src/main.rs
RUN cargo build --release
RUN rm -rf ./src

# Copy the source code and build the application
COPY src ./src
# The last modified attribute of main.rs needs to be updated manually,
# otherwise cargo won't rebuild it.
RUN touch -a -m ./src/main.rs
RUN cargo build --release

FROM rust:1.75.0
RUN apt-get update && apt-get install -y protobuf-compiler ca-certificates openssl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/protobuf-delta-lake-sink /app/protobuf-delta-lake-sink

# Start the application
ENTRYPOINT ["/app/protobuf-delta-lake-sink"]
