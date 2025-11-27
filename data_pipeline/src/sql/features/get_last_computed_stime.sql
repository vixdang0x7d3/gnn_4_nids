-- Get latest processed timestamp for delta processing
SELECT MAX(stime)
FROM features
WHERE computed_at = (SELECT MAX(computed_at) FROM features)
