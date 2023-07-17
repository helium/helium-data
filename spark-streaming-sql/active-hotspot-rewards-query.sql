SELECT 
  max(end_period) as most_recent_ts, 
  b58encodeChecked(gateway_reward.hotspot_key) as hotspot_key 
FROM iot_reward_share
GROUP BY hotspot_key;