-- Keyspace definition for the local fleet telemetry demo.
CREATE KEYSPACE IF NOT EXISTS fleet_telemetry
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;
