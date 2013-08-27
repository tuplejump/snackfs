CREATE KEYSPACE "FS" WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE "FS";

CREATE TABLE inode (
  key blob PRIMARY KEY,
  "706172656e745f70617468" blob,
  "70617468" blob,
  "73656e74696e656c" blob
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='Stores file meta data' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=60 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};

CREATE INDEX parent_path ON inode ("706172656e745f70617468");

CREATE INDEX path ON inode ("70617468");

CREATE INDEX sentinel ON inode ("73656e74696e656c");

CREATE TABLE sblock (
  key blob,
  column1 blob,
  value blob,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='Stores blocks of information associated with a inode' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=60 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
