CREATE KEYSPACE IF NOT EXISTS logeventskeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE logeventskeyspace;

CREATE TABLE IF NOT EXISTS page_views (page VARCHAR, views COUNTER, PRIMARY KEY(page));
CREATE TABLE IF NOT EXISTS log_volume_by_minute (timestamp VARCHAR, count COUNTER, PRIMARY KEY(timestamp));
CREATE TABLE IF NOT EXISTS status_counter (status_code INT, count COUNTER, PRIMARY KEY(status_code));
CREATE TABLE IF NOT EXISTS visits_by_country (country VARCHAR, count COUNTER, PRIMARY KEY(country));
