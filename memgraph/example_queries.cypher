// Artifish Memgraph - Example Queries
// This file contains useful queries for exploring the Bluesky social network

// Get a user's profile by handle
MATCH (u:User {handle: 'boss-c.art.ifi.sh'})
RETURN u;

// Count total users and follows
MATCH (u:User)
RETURN count(u) as total_users;

MATCH ()-[r:FOLLOWS]->()
RETURN count(r) as total_follows;

// Find users with most followers
MATCH (follower:User)-[:FOLLOWS]->(u:User)
RETURN u.handle as handle, u.display_name as name, count(follower) as followers
ORDER BY followers DESC
LIMIT 20;

// Find users with most follows
MATCH (u:User)-[:FOLLOWS]->(following:User)
RETURN u.handle as handle, u.display_name as name, count(following) as following
ORDER BY following DESC
LIMIT 20;

// Find mutual follows for a specific user
MATCH (u:User {handle: 'boss-c.art.ifi.sh'})-[:FOLLOWS]->(following:User)-[:FOLLOWS]->(u)
RETURN following.handle as handle, following.display_name as name;

// Find users two hops away (friends of friends)
MATCH (u:User {handle: 'boss-c.art.ifi.sh'})-[:FOLLOWS]->(following:User)-[:FOLLOWS]->(friend_of_friend:User)
WHERE NOT (u)-[:FOLLOWS]->(friend_of_friend)
AND u <> friend_of_friend
RETURN DISTINCT friend_of_friend.handle as handle, friend_of_friend.display_name as name,
       count(DISTINCT following) as common_connections
ORDER BY common_connections DESC
LIMIT 20;

// Find important users using PageRank algorithm
CALL pagerank.get() YIELD node, rank
WHERE node:User
RETURN node.handle as handle, node.display_name as name, rank as importance
ORDER BY importance DESC
LIMIT 20;

// Find communities using the Louvain algorithm
CALL louvain.get() YIELD node, community
WHERE node:User
RETURN community, count(*) as size, collect(node.handle)[0..5] as sample_users
ORDER BY size DESC
LIMIT 10;

// Find the shortest path between two users
MATCH p = shortestPath(
  (u1:User {handle: 'boss-c.art.ifi.sh'})-[:FOLLOWS*]-(u2:User {handle: 'target.handle'})
)
RETURN p;

// Calculate user stats
MATCH (u:User)
OPTIONAL MATCH (u)-[:FOLLOWS]->(following:User)
OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(u)
WITH u, 
     count(DISTINCT following) as following_count,
     count(DISTINCT follower) as follower_count
OPTIONAL MATCH (u)-[:FOLLOWS]->(mutual:User)-[:FOLLOWS]->(u)
WITH u, following_count, follower_count, count(DISTINCT mutual) as mutual_count
RETURN u.handle as handle, 
       u.display_name as name,
       following_count,
       follower_count,
       mutual_count,
       CASE 
         WHEN follower_count > 0 THEN toFloat(mutual_count) / follower_count 
         ELSE 0 
       END as reciprocity
ORDER BY follower_count DESC
LIMIT 20;

// Find potential follow recommendations for a user
MATCH (u:User {handle: 'boss-c.art.ifi.sh'})-[:FOLLOWS]->(following:User)-[:FOLLOWS]->(recommendation:User)
WHERE NOT (u)-[:FOLLOWS]->(recommendation)
AND u <> recommendation
RETURN recommendation.handle as handle, 
       recommendation.display_name as name,
       count(DISTINCT following) as common_connections,
       size((recommendation)-[:FOLLOWS]->()) as recommendation_follows,
       size(()<-[:FOLLOWS]-(recommendation)) as recommendation_followers
ORDER BY common_connections DESC, recommendation_followers DESC
LIMIT 20;
