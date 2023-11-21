// Create a unique constraint on the author_id for Author nodes
CREATE CONSTRAINT author_id_uniqueness IF NOT EXISTS FOR (a:Author) REQUIRE a.author_id IS UNIQUE;

// Create a unique constraint on the category_id for Category nodes
CREATE CONSTRAINT category_id_uniqueness IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE;

// Create a unique constraint on the paper_id for Paper nodes
CREATE CONSTRAINT paper_id_uniqueness IF NOT EXISTS FOR (p:Paper) REQUIRE p.paper_id IS UNIQUE;

// Create a unique constraint on the version_id for Version nodes (if applicable)
CREATE CONSTRAINT version_id_uniqueness IF NOT EXISTS FOR (v:Version) REQUIRE v.version_id IS UNIQUE;

// Create indexes on frequently queried properties
CREATE INDEX author_name_index IF NOT EXISTS FOR (a:Author) ON (a.last_name, a.first_name, a.middle_name);
CREATE INDEX category_name_index IF NOT EXISTS FOR (c:Category) ON (c.category_name);
CREATE INDEX paper_update_date_index IF NOT EXISTS FOR (p:Paper) ON (p.update_date);
// ... Add more indexes as necessary for your queries
