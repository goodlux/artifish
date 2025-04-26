-- Add follows_last_updated_at field to bluesky_accounts table
ALTER TABLE bluesky_accounts ADD COLUMN follows_last_updated_at TIMESTAMPTZ;

-- Add index for efficient querying of accounts by follows_last_updated_at
CREATE INDEX idx_bluesky_accounts_follows_last_updated_at ON bluesky_accounts (follows_last_updated_at);

-- Add follow_status field to follows table to track if a follow is active or unfollowed
ALTER TABLE follows ADD COLUMN follow_status TEXT DEFAULT 'active';

-- Add unfollowed_at timestamp to follows table
ALTER TABLE follows ADD COLUMN unfollowed_at TIMESTAMPTZ;

-- Add index for querying follows by status
CREATE INDEX idx_follows_follow_status ON follows (follow_status);

-- Create view for finding recent follows
CREATE OR REPLACE VIEW recent_follows AS
SELECT 
  f.follower_did,
  f.following_did,
  a1.handle AS follower_handle,
  a2.handle AS following_handle,
  f.created_at,
  f.last_verified_at
FROM 
  follows f
JOIN 
  bluesky_accounts a1 ON f.follower_did = a1.did
JOIN 
  bluesky_accounts a2 ON f.following_did = a2.did
WHERE 
  f.follow_status = 'active' 
  AND f.created_at > (CURRENT_TIMESTAMP - INTERVAL '7 days')
ORDER BY 
  f.created_at DESC;

-- Create view for finding unfollows
CREATE OR REPLACE VIEW recent_unfollows AS
SELECT 
  f.follower_did,
  f.following_did,
  a1.handle AS follower_handle,
  a2.handle AS following_handle,
  f.unfollowed_at
FROM 
  follows f
JOIN 
  bluesky_accounts a1 ON f.follower_did = a1.did
JOIN 
  bluesky_accounts a2 ON f.following_did = a2.did
WHERE 
  f.follow_status = 'unfollowed' 
  AND f.unfollowed_at > (CURRENT_TIMESTAMP - INTERVAL '7 days')
ORDER BY 
  f.unfollowed_at DESC;
