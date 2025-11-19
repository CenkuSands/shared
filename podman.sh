podman run --rm --security-opt label=disable -v /var/lib/kafka-streams/_confluent-ksql-default_query_CTAS_SCL_QA3_M_PROMOTIONPLATFORM_ENROLLEDPROMO_EXPLODE_RESERIALIZE_TAB_5962/0_0/rocksdb/KsqlTopic-Reduce:/data:ro docker.io/library/rocksdb-tool ldb --db=/data scan



commands MUST specify --db=<full_path_to_db_directory> when necessary

commands can optionally specify
  --env_uri=<uri_of_environment> or --fs_uri=<uri_of_filesystem> if necessary
  --secondary_path=<secondary_path> to open DB as secondary instance. Operations not supported in secondary instance will fail.

  --leader_path=<leader_path> to open DB as a follower instance. Operations not supported in follower instance will fail.

The following optional parameters control if keys/values are input/output as hex or as plain strings:
  --key_hex : Keys are input/output as hex
  --value_hex : Values are input/output as hex
  --hex : Both keys and values are input/output as hex

The following optional parameters control the database internals:
  --column_family=<string> : name of the column family to operate on. default: default column family
  --ttl with 'put','get','scan','dump','query','batchput' : DB supports ttl and value is internally timestamp-suffixed
  --try_load_options : Try to load option file from DB. Default to true if db is specified and not creating a new DB and not open as TTL DB. Can be set to false explicitly.
  --disable_consistency_checks : Set options.force_consistency_checks = false.
  --ignore_unknown_options : Ignore unknown options when loading option file.
  --bloom_bits=<int,e.g.:14>
  --fix_prefix_len=<int,e.g.:14>
  --compression_type=<no|snappy|zlib|bzip2|lz4|lz4hc|xpress|zstd>
  --compression_max_dict_bytes=<int,e.g.:16384>
  --block_size=<block_size_in_bytes>
  --auto_compaction=<true|false>
  --db_write_buffer_size=<int,e.g.:16777216>
  --write_buffer_size=<int,e.g.:4194304>
  --file_size=<int,e.g.:2097152>
  --enable_blob_files : Enable key-value separation using BlobDB
  --min_blob_size=<int,e.g.:2097152>
  --blob_file_size=<int,e.g.:2097152>
  --blob_compression_type=<no|snappy|zlib|bzip2|lz4|lz4hc|xpress|zstd>
  --enable_blob_garbage_collection : Enable blob garbage collection
  --blob_garbage_collection_age_cutoff=<double,e.g.:0.25>
  --blob_garbage_collection_force_threshold=<double,e.g.:0.25>
  --blob_compaction_readahead_size=<int,e.g.:2097152>
  --read_timestamp=<uint64_ts, e.g.:323> : read timestamp, required if column family enables timestamp, otherwise invalid if provided.

Data Access Commands:
  put <key> <value> [--create_if_missing] [--ttl]
  put_entity <key> <column1_name>:<column1_value> <column2_name>:<column2_value> <...> [--create_if_missing] [--ttl]
  get <key> [--read_timestamp=<uint64_ts>]  [--ttl]
  get_entity <key> [--read_timestamp=<uint64_ts>]  [--ttl]
  multi_get <key_1> <key_2> <key_3> ... [--read_timestamp=<uint64_ts>]
  multi_get_entity <key_1> <key_2> <key_3> ... [--read_timestamp=<uint64_ts>]
  batchput <key> <value> [<key> <value>] [..] [--create_if_missing] [--ttl]
  scan [--from] [--to]  [--ttl] [--timestamp] [--max_keys=<N>q]  [--start_time=<N>:- is inclusive] [--end_time=<N>:- is exclusive] [--no_value] [--read_timestamp=<uint64_ts>]  [--get_write_unix_time]
  delete <key>
  singledelete <key>
  deleterange <begin key> <end key>
  query [--ttl]
    Starts a REPL shell.  Type help for list of available commands.
  approxsize [--from] [--to]
  checkconsistency
  list_file_range_deletes [--max_keys=<N>] : print tombstones in SST files.


Admin Commands:
  dump_wal --walfile=<write_ahead_log_file_path_or_directory> [--db=<db_path>] [--header]  [--print_value]  [--only_print_seqno_gaps] (only correct if not using pessimistic transactions) [--write_committed=true|false]
  compact [--from] [--to]
  reduce_levels --new_levels=<New number of levels> [--print_old_levels]
  change_compaction_style --old_compaction_style=<Old compaction style: 0 for level compaction, 1 for universal compaction> --new_compaction_style=<New compaction style: 0 for level compaction, 1 for universal compaction>
  dump [--from] [--to]  [--ttl] [--max_keys=<N>] [--timestamp] [--count_only] [--count_delim=<char>] [--stats] [--bucket=<N>] [--start_time=<N>:- is inclusive] [--end_time=<N>:- is exclusive] [--path=<path_to_a_file>] [--decode_blob_index] [--dump_uncompressed_blobs]
  load [--create_if_missing] [--disable_wal] [--bulk_load] [--compact]
  manifest_dump [--verbose] [--json] [--path=<path_to_manifest_file>]
  compaction_progress_dump [--path=<path_to_compaction_progress_file>]
  update_manifest [--update_temperatures]      MUST NOT be used on a live DB.
  file_checksum_dump [--path=<path_to_manifest_file>]
  get_property <property_name>
  list_column_families
  create_column_family --db=<db_path> <new_column_family_name>
  drop_column_family --db=<db_path> <column_family_name_to_drop>
  dump_live_files [--decode_blob_index]  [--dump_uncompressed_blobs]
  idump [--from] [--to]  [--input_key_hex] [--max_keys=<N>] [--count_only] [--count_delim=<char>] [--stats] [--decode_blob_index]
  list_live_files_metadata [--sort_by_filename]
  repair [--verbose]
  backup [--backup_env_uri | --backup_fs_uri]  [--backup_dir]  [--num_threads]  [--stderr_log_level=<int (InfoLogLevel)>]
  restore [--backup_env_uri | --backup_fs_uri]  [--backup_dir]  [--num_threads]  [--stderr_log_level=<int (InfoLogLevel)>]
  checkpoint [--checkpoint_dir]
  write_extern_sst <output_sst_path>
  ingest_extern_sst <input_sst_path> [--move_files]  [--snapshot_consistency]  [--allow_global_seqno]  [--allow_blocking_flush]  [--ingest_behind]  [--write_global_seqno]
  unsafe_remove_sst_file <SST file number>      MUST NOT be used on a live DB.
