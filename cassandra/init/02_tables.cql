-- Table definitions for fleet telemetry, events, and window aggregates.
CREATE TABLE IF NOT EXISTS fleet_telemetry.vehicles (
  vehicle_id TEXT,
  plate_number TEXT,
  brand TEXT,
  model TEXT,
  year INT,
  persona TEXT,
  fuel_type TEXT,
  fleet_group TEXT,
  home_city TEXT,
  PRIMARY KEY ((vehicle_id))
);

CREATE TABLE IF NOT EXISTS fleet_telemetry.telemetry_by_vehicle_time (
  vehicle_id TEXT,
  event_time TIMESTAMP,
  trip_id UUID,
  lat DOUBLE,
  lon DOUBLE,
  heading_deg DOUBLE,
  speed_kmh FLOAT,
  gear INT,
  rpm INT,
  throttle_pct FLOAT,
  maf_g_per_s FLOAT,
  engine_temp_c FLOAT,
  coolant_temp_c FLOAT,
  intake_air_temp_c FLOAT,
  fuel_pct FLOAT,
  fuel_consumed_l FLOAT,
  odometer_km FLOAT,
  ignition_on BOOLEAN,
  PRIMARY KEY ((vehicle_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS fleet_telemetry.events_by_vehicle_time (
  event_id UUID,
  vehicle_id TEXT,
  trip_id UUID,
  event_time TIMESTAMP,
  event_type TEXT,
  detail TEXT,
  PRIMARY KEY ((vehicle_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS fleet_telemetry.vehicle_window_aggregates (
  vehicle_id TEXT,
  window_start TIMESTAMP,
  avg_speed_kmh FLOAT,
  max_speed_kmh FLOAT,
  harsh_event_count INT,
  PRIMARY KEY ((vehicle_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS fleet_telemetry.fleet_window_aggregates (
  day_bucket DATE,
  window_start TIMESTAMP,
  vehicles_active INT,
  fleet_avg_speed_kmh FLOAT,
  total_harsh_events INT,
  PRIMARY KEY ((day_bucket), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
