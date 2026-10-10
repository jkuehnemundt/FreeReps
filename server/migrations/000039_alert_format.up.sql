-- The wire format of the alert channel: 'ntfy' posts a text message with
-- ntfy's title, priority and tags headers, 'kuma-json' the Uptime Kuma webhook
-- object that was the only format before this column existed.
--
-- An existing row keeps 'kuma-json', so a deployment whose consumer parses that
-- object receives the same messages after the upgrade. A row written later
-- without the column — none in this code, which always names it — gets 'ntfy',
-- the format a person subscribed to the topic can read.
ALTER TABLE alert_settings
    ADD COLUMN format TEXT NOT NULL DEFAULT 'kuma-json'
    CHECK (format IN ('ntfy', 'kuma-json'));
ALTER TABLE alert_settings ALTER COLUMN format SET DEFAULT 'ntfy';
