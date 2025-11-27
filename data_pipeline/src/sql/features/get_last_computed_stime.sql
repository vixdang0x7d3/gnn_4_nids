-- Get latest processed timestamp for delta processing
SELECT MAX(stime)
FROM og_features
WHERE computed_at = (SELECT MAX(computed_at) FROM og_features)
