WITH raw AS (
  SELECT 
    poc_id,
    date,
    beacon_report,
    explode_outer(selected_witnesses) as witness,
    true as selected
    FROM lora_poc_v1
  UNION ALL 
  SELECT poc_id,
    date,
    beacon_report,
    explode_outer(unselected_witnesses) as witness,
    false as selected
  FROM lora_poc_v1
)
SELECT
    date,
    selected,
    base64(poc_id) as poc_id,
    struct(
      from_unixtime(beacon_report.received_timestamp / 1000) as received_timestamp_millis,
      cast(beacon_report.location as decimal(23, 0)) location,
      (beacon_report.hex_scale / 10000) as scale,
      b58encode(beacon_report.report.pub_key) as pub_key,
      base64(beacon_report.report.local_entropy) as local_entropy,
      base64(beacon_report.report.remote_entropy) as remote_entropy,
      base64(beacon_report.report.data) as data,
      beacon_report.report.timestamp as timestamp_nanos,
      beacon_report.report.tmst as tmst,
      (beacon_report.report.tx_power / 10) as tx_power_dbm,    beacon_report.report.channel,
      beacon_report.report.frequency as frequency_hz,
      beacon_report.report.datarate,
      beacon_report.report.signature,
      (beacon_report.reward_unit / 10000) as reward_unit,
      (beacon_report.gain / 10) as gain_dbi,
      beacon_report.elevation
     ) as beacon,
     struct(
      from_unixtime(witness.received_timestamp / 1000) as received_timestamp_millis,
      cast(witness.location as decimal(23, 0)) location,
      b58encode(witness.report.pub_key) as pub_key,
      (witness.hex_scale / 10000) as scale,
      witness.status,
      base64(witness.report.data) as data,
      witness.report.timestamp as timestamp_nanos,
      witness.report.tmst as tmst,
      witness.report.signal as signal_ddbm,
      witness.report.frequency as frequency_hz,
      witness.report.snr,
      witness.report.datarate,
      base64(witness.report.signature) as signature,
      (witness.reward_unit / 10000) as reward_unit,
      witness.invalid_reason,
      witness.participant_side,
      (witness.gain / 10) as gain_dbi,
      witness.elevation
    ) as witness    
FROM raw