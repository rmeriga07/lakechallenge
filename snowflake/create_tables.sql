
use accountadmin;

USE DATABASE uberdb;
USE SCHEMA uber;


CREATE OR REPLACE TABLE  uber_stg
(   origin_movement_id              STRING,
    origin_display_name             STRING,
    origin_geometry                 STRING,
    destination_movement_id         STRING,
    destination_display_name        STRING,
    destination_geometry             STRING,
    date_range                      STRING,
    mean_travel_time_seconds        STRING,
    lower_bound_travel_time_seconds STRING,
    upper_bound_travel_time_seconds STRING,
    source_file                     STRING,
    rawload_ts                      TIMESTAMP,
    start_dt                        STRING,
    end_dt                          STRING,
    city                            STRING,
    sf_ts                           TIMESTAMP
);



CREATE OR REPLACE TABLE bf_travel
(
    travel_key               STRING NOT NULL,
    travel_id                STRING NOT NULL,
    city                     STRING,
    business_dt              DATE   NOT NULL,
    origin_key               STRING,
    destination_key          STRING,
    mean_travel_time         INT,
    upper_bound_travel_time  INT,
    lower_bound_travel_time  INT,
    from_date                TIMESTAMP NOT NULL,
    to_date                  TIMESTAMP NOT NULL,
    current_version          CHAR(1)   NOT NULL
);

CREATE OR REPLACE bd_calendar
(
    business_dt  DATE
);


CREATE OR REPLACE bd_origin
(
    origin_key              STRING NOT NULL,
    origin_movement_id      INT NOT NULL,
    origin_display_name     STRING,
    origin_geometry         STRING,
    from_date               TIMESTAMP NOT NULL,
    to_date                 TIMESTAMP NOT NULL,
    current_version         CHAR(1) NOT NULL
);

CREATE OR REPLACE bd_destination
(
    destination_key              STRING NOT NULL,
    destination_movement_id      INT NOT NULL,
    destination_display_name     STRING,
    destination_geometry         STRING,
    from_date               TIMESTAMP NOT NULL,
    to_date                 TIMESTAMP NOT NULL,
    current_version         CHAR(1) NOT NULL
);


CREATE OR REPLACE bc_travel
(
    travel_key               STRING NOT NULL,
    travel_id                STRING NOT NULL,
    city                     STRING,
    business_dt              DATE   NOT NULL,
    mean_travel_time         INT,
    upper_bound_travel_time  INT,
    lower_bound_travel_time  INT,
    upper_bound_mean_diff    INT,
    lower_bound_mean_diff    INT
    
);






