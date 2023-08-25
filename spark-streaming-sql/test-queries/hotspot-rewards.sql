SELECT
  max(end_period) as most_recent_ts, 
  entity_key,
  'iot' as sub_dao
FROM iot_hotspot_rewards
WHERE date >= date_sub(current_date(), 62) -- Pull a little more than two months so we also get transitioning hotspots
      AND (beacon_amount > 0 OR witness_amount > 0 OR dc_transfer_amount > 0)
GROUP BY entity_key
UNION ALL
SELECT
  max(end_period) as most_recent_ts, 
  entity_key,
  'mobile' as sub_dao
FROM mobile_hotspot_rewards
WHERE date >= date_sub(current_date(), 62) -- Pull a little more than two months so we also get transitioning hotspots
      AND (poc_amount > 0 OR dc_transfer_amount > 0)
GROUP BY entity_key;
