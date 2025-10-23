-- Dimension: dim_date (Delta Table with Surrogate Key)
-- Note: Run 00_config.sql first to set catalog_name and schema_name variables

CREATE TABLE IF NOT EXISTS ${schema_name}.dim_date (
  date_id BIGINT GENERATED ALWAYS AS IDENTITY,
  date_key DATE,
  full_date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT,
  weekday_name STRING,
  is_weekend BOOLEAN,
  PRIMARY KEY (date_id)
) USING DELTA;

-- Populate dim_date with surrogate key
CREATE OR REPLACE TEMP VIEW date_src AS
SELECT explode(sequence(date('1992-01-01'), date('1998-12-31'), interval 1 day)) AS calendar_date;

INSERT INTO ${schema_name}.dim_date (date_key, full_date, year, quarter, month, day, weekday_name, is_weekend)
SELECT
  calendar_date,
  calendar_date,
  year(calendar_date),
  quarter(calendar_date),
  month(calendar_date),
  day(calendar_date),
  date_format(calendar_date, 'EEEE'),
  is_weekend(calendar_date)
FROM date_src;