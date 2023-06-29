WITH 
    beacon_report AS (
        SELECT
            beacon.pub_key as beacon_pub_key,
            MAX(beacon.received_timestamp) as most_recent_beacon_received_timestamp
        FROM
            test
        GROUP BY
            beacon_pub_key
    ),
    witness_report AS (
        SELECT
            witness.pub_key as witness_pub_key,
            MAX(witness.received_timestamp) as most_recent_witness_received_timestamp
        FROM
            test
        GROUP BY witness_pub_key
    )
SELECT * FROM beacon_report
FULL OUTER JOIN witness_report ON witness_report.witness_pub_key == beacon_report.beacon_pub_key