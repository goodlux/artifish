// Artifish Memgraph Schema Setup
// This file contains the Cypher queries to set up the initial schema for Artifish

// Set to analytical mode for better import performance
STORAGE MODE IN_MEMORY_ANALYTICAL;

// Create constraints for main entity types to ensure unique identifiers
CREATE CONSTRAINT ON (u:User) ASSERT u.did IS UNIQUE;
CREATE CONSTRAINT ON (p:Post) ASSERT p.uri IS UNIQUE;
CREATE CONSTRAINT ON (i:Interest) ASSERT i.name IS UNIQUE;
CREATE CONSTRAINT ON (a:Artifish) ASSERT a.artifish_id IS UNIQUE;

// Create indexes for better query performance
CREATE INDEX ON :User(handle);
CREATE INDEX ON :Post(author_did);
CREATE INDEX ON :Post(created_at);
CREATE INDEX ON :Interest(category);

// Create sample data for testing (optional - comment out in production)
// Uncomment and modify as needed for testing

/*
// Create a few sample users
CREATE (a:User {did: "did:plc:sample1", handle: "user1.bsky.social", display_name: "User One"})
CREATE (b:User {did: "did:plc:sample2", handle: "user2.bsky.social", display_name: "User Two"})
CREATE (c:User {did: "did:plc:sample3", handle: "user3.bsky.social", display_name: "User Three"})

// Create some follow relationships
CREATE (a)-[:FOLLOWS {created_at: datetime()}]->(b)
CREATE (b)-[:FOLLOWS {created_at: datetime()}]->(c)
CREATE (c)-[:FOLLOWS {created_at: datetime()}]->(a)

// Create some interests
CREATE (i1:Interest {name: "ai", category: "technology"})
CREATE (i2:Interest {name: "music", category: "entertainment"})
CREATE (i3:Interest {name: "cooking", category: "lifestyle"})

// Associate interests with users
CREATE (a)-[:INTERESTED_IN {confidence: 0.9, last_updated: datetime()}]->(i1)
CREATE (a)-[:INTERESTED_IN {confidence: 0.7, last_updated: datetime()}]->(i2)
CREATE (b)-[:INTERESTED_IN {confidence: 0.8, last_updated: datetime()}]->(i3)
CREATE (c)-[:INTERESTED_IN {confidence: 0.6, last_updated: datetime()}]->(i1)
*/

// Switch back to transactional mode when done with bulk imports
STORAGE MODE IN_MEMORY_TRANSACTIONAL;

// Verify setup
MATCH (n) RETURN DISTINCT labels(n) as NodeTypes, count(*) as Count;
