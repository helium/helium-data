#Data from S3

Run the following command

aws s3 sync s3://foundation-data-lake-requester-pays/bronze/packet_router_packet_report_v1/ . --exclude "*date=1970-01-01/*"
