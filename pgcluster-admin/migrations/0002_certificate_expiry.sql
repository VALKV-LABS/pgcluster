-- When each node's certificate expires. Rows from before this are renewed first.
ALTER TABLE nodes ADD COLUMN cert_not_after timestamptz;
