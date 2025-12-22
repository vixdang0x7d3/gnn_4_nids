-- Calculate safe horizon for delta processing
-- safe_horizon = min(ts accross all sources) - slack
SELECT MIN(max_event_ts) - :slack AS safe_horizon
FROM source_watermarks
WHERE source LIKE 'raw_%';
