SELECT
  date,
  start_period,
  end_period,
  b58encodeChecked(gateway_reward.hotspot_key) as entity_key,
  gateway_reward.beacon_amount as beacon_amount,
  gateway_reward.witness_amount as witness_amount,
  gateway_reward.dc_transfer_amount as dc_transfer_amount
FROM iot_reward_share