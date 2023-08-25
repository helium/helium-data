SELECT
  date,
  start_period,
  end_period,
  coalesce(b58encodeChecked(radio_reward.hotspot_key), b58encodeChecked(gateway_reward.hotspot_key)) as entity_key,
  gateway_reward.dc_transfer_reward as dc_transfer_amount,
  radio_reward.poc_reward as poc_amount,
  radio_reward.coverage_points as coverage_points,
  radio_reward.cbsd_id as cbsd_id
FROM mobile_reward_share
