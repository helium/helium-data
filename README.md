
# Install minio and create buckets
1. Install the local minio server and visit localhost:9000 in your browser
2. Create an `iot-ingest` and `delta` bucket
3. Put your gzip files in `iot-ingest` and you can now run the `protobuf-delta-lake-sink` tool

# Set up scala in vscode
1. Open the scala related subdirectory in a new vscode. E.g. open spark-streaming-sql in vscode
2. Install Metals vscode extension
3. Import build under build commands in the extension page
4. Run the project from the vscode run menu