-- Additive offline maintenance only; approval required before production use.
BEGIN IMMEDIATE;
CREATE INDEX IF NOT EXISTS audit_observer_windows ON audit_events(source,event_type,occurred,producer_storage);
CREATE INDEX IF NOT EXISTS jobs_observer_all ON jobs(source,state,retry_count,lease_until);
CREATE INDEX IF NOT EXISTS jobs_observer_fifo ON jobs(source,queued_at,id) WHERE state='queued';
CREATE INDEX IF NOT EXISTS jobs_observer_heads ON jobs(source,CASE WHEN source='nextcloud' AND nextcloud_priority='new_upload' THEN 0 ELSE 1 END,queued_at,id) WHERE state='queued';
CREATE INDEX IF NOT EXISTS jobs_observer_public ON jobs(source,state,retry_count,lease_until) WHERE producer_attempt_id IS NULL AND input_storage_mode='broker_temporary' AND result_storage_mode='broker_temporary';
CREATE INDEX IF NOT EXISTS jobs_observer_public_fifo ON jobs(source,queued_at,id) WHERE state='queued' AND producer_attempt_id IS NULL AND input_storage_mode='broker_temporary' AND result_storage_mode='broker_temporary';
CREATE INDEX IF NOT EXISTS jobs_observer_public_heads ON jobs(source,CASE WHEN source='nextcloud' AND nextcloud_priority='new_upload' THEN 0 ELSE 1 END,queued_at,id) WHERE state='queued' AND producer_attempt_id IS NULL AND input_storage_mode='broker_temporary' AND result_storage_mode='broker_temporary';
COMMIT;
