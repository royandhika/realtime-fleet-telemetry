-- Seed data for the two demo vehicles.
INSERT INTO fleet_telemetry.vehicles (vehicle_id, plate_number, brand, model, year, persona, fuel_type, fleet_group, home_city)
  VALUES ('veh_0001', 'B 1234 DEL', 'Toyota',     'Innova Reborn', 2021, 'delivery_van',  'Pertamax 95', 'jakarta', 'Jakarta');
INSERT INTO fleet_telemetry.vehicles (vehicle_id, plate_number, brand, model, year, persona, fuel_type, fleet_group, home_city)
  VALUES ('veh_0002', 'D 5678 AZR', 'Mitsubishi', 'Xpander',       2020, 'city_commuter', 'Pertamax 92', 'bandung', 'Bandung');
