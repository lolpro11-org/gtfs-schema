use std::{fs, path::PathBuf};
mod dmfr;
use futures::{stream::FuturesUnordered, StreamExt};
use gtfs_structures::{Availability, BikesAllowedType, ContinuousPickupDropOff, DirectionType, Exception, Gtfs, LocationType, PaymentMethod, RouteType, Transfers};
use serde_derive::Serialize;
use tokio::task;
use tokio_postgres::{Client, NoTls};
use serde_json::{json, Value};

#[derive(Serialize)]
struct GeoJsonProperties {
    sequence: usize,
    dist_traveled: Option<f32>,
}

#[derive(Serialize)]
struct GeoJsonPoint {
    #[serde(rename = "type")]
    type_: String,
    coordinates: [f64; 2],
    properties: GeoJsonProperties,
}

#[derive(Serialize)]
struct GeoJsonFeatureCollection {
    #[serde(rename = "type")]
    type_: String,
    features: Vec<Value>,
}

async fn makedb(client: &Client) {
    client.batch_execute("
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS hstore;
    ").await.unwrap();

    client.batch_execute("
    DROP TABLE IF EXISTS
        agency,
        stops,
        routes,
        trips,
        stop_times,
        attributions,
        calendar,
        calendar_dates,
        fare_attributes,
        fare_rules,
        fare_media,
        fare_products,
        areas,
        stop_areas,
        networks,
        route_networks,
        shapes,
        frequencies,
        timeframes,
        transfers,
        pathways,
        levels,
        feed_info,
        translations;
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE agency (
            agency_id text NULL,
            agency_name text NOT NULL,
            agency_url text NOT NULL,
            agency_timezone text NOT NULL,
            agency_lang text NULL,
            agency_phone text NULL,
            agency_fare_url text NULL,
            agency_email text NULL,
            onestop_feed_id text NOT NULL PRIMARY KEY
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE levels (
            level_id text PRIMARY KEY,
            level_index double precision NOT NULL,
            level_name text NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stops (
            stop_id text NOT NULL,
            stop_code text NULL,
            stop_name text NULL CHECK (location_type >= 0 AND location_type <= 2 AND stop_name IS NOT NULL OR location_type > 2),
            tts_stop_name text NULL,
            stop_desc text NULL,
            stop_lat double precision NULL CHECK (location_type >= 0 AND location_type <= 2 AND stop_name IS NOT NULL OR location_type > 2),
            stop_lon double precision NULL CHECK (location_type >= 0 AND location_type <= 2 AND stop_name IS NOT NULL OR location_type > 2),
            zone_id text NULL,
            stop_url text NULL,
            location_type integer NULL CHECK (location_type >= 0 AND location_type <= 4),
            parent_station text NULL CHECK (location_type IS NULL OR location_type = 0 OR location_type = 1 AND parent_station IS NULL OR location_type >= 2 AND location_type <= 4 AND parent_station IS NOT NULL),
            stop_timezone text NULL,
            wheelchair_boarding integer NULL CHECK (wheelchair_boarding >= 0 AND wheelchair_boarding <= 2 OR wheelchair_boarding IS NULL),
            level_id text NULL REFERENCES levels ON DELETE CASCADE ON UPDATE CASCADE,
            platform_code text NULL,
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, stop_id)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE routes (
            route_id text,
            agency_id text NULL,
            route_short_name text NULL,
            route_long_name text NULL CHECK (route_short_name IS NOT NULL OR route_long_name IS NOT NULL),
            route_desc text NULL,
            route_type integer NOT NULL,
            route_url text NULL,
            route_color text NULL,
            route_text_color text NULL,
            route_sort_order integer NULL CHECK (route_sort_order >= 0),
            continuous_pickup integer NULL,
            continuous_drop_off integer NULL,
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, route_id)
            --FOREIGN KEY (agency_id) REFERENCES agency(onestop_feed_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE trips (
            route_id text NOT NULL,
            service_id text NOT NULL,
            trip_id text NOT NULL,
            trip_headsign text NULL,
            trip_short_name text NULL,
            direction_id integer NULL CHECK (direction_id >= 0 AND direction_id <= 1),
            block_id text NULL,
            shape_id text NULL,
            wheelchair_accessible integer NULL CHECK (wheelchair_accessible >= 0 AND wheelchair_accessible <= 2),
            bikes_allowed integer NULL CHECK (bikes_allowed >= 0 AND bikes_allowed <= 2),
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, trip_id),
            FOREIGN KEY (onestop_feed_id, route_id) REFERENCES routes(onestop_feed_id, route_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stop_times (
            trip_id text NOT NULL,
            onestop_feed_id text NOT NULL,
            arrival_time interval NULL,
            departure_time interval NOT NULL,
            stop_id text NOT NULL,
            stop_sequence integer NOT NULL CHECK (stop_sequence >= 0),
            stop_headsign text NULL,
            pickup_type integer NOT NULL CHECK (pickup_type >= 0 AND pickup_type <= 3),
            drop_off_type integer NOT NULL CHECK (drop_off_type >= 0 AND drop_off_type <= 3),
            continuous_pickup integer NULL,
            continuous_drop_off integer NULL,
            shape_dist_traveled double precision NULL CHECK (shape_dist_traveled >= 0.0),
            timepoint boolean NULL,
            PRIMARY KEY (onestop_feed_id, trip_id),
            FOREIGN KEY (onestop_feed_id, stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (onestop_feed_id, trip_id) REFERENCES trips(onestop_feed_id, trip_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE calendar (
            service_id text NOT NULL,
            monday boolean NOT NULL,
            tuesday boolean NOT NULL,
            wednesday boolean NOT NULL,
            thursday boolean NOT NULL,
            friday boolean NOT NULL,
            saturday boolean NOT NULL,
            sunday boolean NOT NULL,
            start_date date NOT NULL,
            end_date date NOT NULL,
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, service_id)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE calendar_dates (
            service_id text NOT NULL,
            date date NOT NULL,
            exception_type integer NOT NULL CHECK (exception_type >= 1 AND exception_type <= 2),
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, service_id)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_attributes (
            fare_id text NOT NULL,
            price text NOT NULL, --double precision NOT NULL CHECK (price >= 0.0),
            currency_type text NOT NULL,
            payment_method integer NOT NULL CHECK (payment_method >= 0 AND payment_method <= 1),
            transfers integer NULL CHECK (transfers >= 0 AND transfers <= 5),
            agency_id text NULL,
            transfer_duration integer NULL CHECK (transfer_duration >= 0),
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, fare_id)
            --FOREIGN KEY (agency_id) REFERENCES agency(onestop_feed_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_rules (
            fare_id text NOT NULL,
            route_id text NULL,
            origin_id text NULL,
            destination_id text NULL,
            contains_id text NULL,
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, fare_id),
            FOREIGN KEY (onestop_feed_id, fare_id) REFERENCES fare_attributes(onestop_feed_id, fare_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (onestop_feed_id, route_id) REFERENCES routes(onestop_feed_id, route_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE timeframes (
            timeframe_group_id text NOT NULL,
            start_time interval NULL,
            end_time interval NULL,
            service_id text NOT NULL,
            onestop_feed_id text NOT NULL,
            FOREIGN KEY (onestop_feed_id, service_id) REFERENCES calendar(onestop_feed_id, service_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_media (
            fare_media_id text PRIMARY KEY,
            fare_media_name text NULL,
            fare_media_type integer NOT NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_products (
            fare_product_id text PRIMARY KEY,
            fare_product_name text NULL,
            fare_media_id text REFERENCES fare_media ON DELETE CASCADE ON UPDATE CASCADE,
            amount text NOT NULL,
            currency text NOT NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE areas (
            area_id text PRIMARY KEY,
            area_name text NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stop_areas (
            area_id text NOT NULL REFERENCES areas ON DELETE CASCADE ON UPDATE CASCADE,
            stop_id text NOT NULL,
            onestop_feed_id text NOT NULL,
            FOREIGN KEY (onestop_feed_id, stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE networks (
            network_id text PRIMARY KEY,
            network_name text NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE route_networks (
            network_id text NOT NULL REFERENCES networks ON DELETE CASCADE ON UPDATE CASCADE,
            route_id text NOT NULL,
            onestop_feed_id text NOT NULL,
            FOREIGN KEY (onestop_feed_id, route_id) REFERENCES routes(onestop_feed_id, route_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE shapes (
            shape_id text NOT NULL,
            shape_geojson JSONB NOT NULL,
            onestop_feed_id text NOT NULL,
            PRIMARY KEY (onestop_feed_id, shape_id)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE frequencies (
            trip_id text NOT NULL,
            start_time interval NOT NULL,
            end_time interval NOT NULL,
            headway_secs integer NOT NULL CHECK (headway_secs >= 0),
            exact_times boolean NULL,
            onestop_feed_id text NOT NULL,
            FOREIGN KEY (onestop_feed_id, trip_id) REFERENCES trips(onestop_feed_id, trip_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE transfers (
            from_onestop_feed_id text NOT NULL,
            from_stop_id text NOT NULL,
            to_onestop_feed_id text NOT NULL,
            to_stop_id text NOT NULL,
            transfer_type integer NOT NULL CHECK (transfer_type >= 0 AND transfer_type <= 3),
            min_transfer_time integer NULL CHECK (min_transfer_time >= 0),
            from_route_id text NULL,
            to_route_id text NULL,
            from_trip_id text NULL,
            to_trip_id text NULL,
            FOREIGN KEY (from_onestop_feed_id, from_stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (to_onestop_feed_id, to_stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE pathways (
            pathway_id text PRIMARY KEY,
            from_stop_id text NOT NULL,
            to_stop_id text NOT NULL,
            to_onestop_feed_id text NOT NULL,
            pathway_mode integer NOT NULL CHECK (pathway_mode >= 1 AND pathway_mode <= 7),
            is_bidirectional boolean NOT NULL,
            length double precision NULL CHECK (length >= 0.0),
            traversal_time integer NULL CHECK (traversal_time >= 0),
            stair_count integer NULL,
            max_slope double precision NULL,
            min_width double precision NULL CHECK (min_width >= 0.0),
            signposted_as text NULL,
            reversed_signposted_as text NULL,
            onestop_feed_id text NOT NULL,
            FOREIGN KEY (onestop_feed_id, from_stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (to_onestop_feed_id, to_stop_id) REFERENCES stops(onestop_feed_id, stop_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE feed_info (
            feed_publisher_name text NOT NULL,
            feed_publisher_url text NOT NULL,
            feed_lang text NOT NULL,
            feed_start_date date NULL,
            feed_end_date date NULL,
            feed_version text NULL,
            feed_contact_email text NULL,
            feed_contact_url text NULL,
            default_lang text NULL,
            onestop_feed_id text NOT NULL PRIMARY KEY
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE translations (
            table_name text NOT NULL,
            field_name text NOT NULL,
            language text NOT NULL,
            translation text NOT NULL,
            record_id text NULL,
            record_sub_id text NULL,
            field_value text NULL,
            onestop_feed_id text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE attributions (
            attribution_id text PRIMARY KEY,
            agency_id text NOT NULL,
            route_onestop_feed_id text NULL,
            route_id text NULL,
            trip_id text NULL,
            organization_name text NOT NULL,
            is_producer integer NULL,
            is_operator integer NULL,
            is_authority integer NULL,
            attribution_url text NULL,
            attribution_phone text NULL,
            attribution_email text NULL,
            onestop_feed_id text NOT NULL,
            --FOREIGN KEY (agency_id) REFERENCES agency(onestop_feed_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (route_onestop_feed_id, route_id) REFERENCES routes(onestop_feed_id, route_id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (onestop_feed_id, trip_id) REFERENCES trips(onestop_feed_id, trip_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    
    client.batch_execute("
        CREATE OR REPLACE
        FUNCTION busonly(z integer, x integer, y integer)
        RETURNS bytea AS $$
        DECLARE
        mvt bytea;
        BEGIN
        SELECT INTO mvt ST_AsMVT(tile, 'busonly', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(linestring, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
            FROM gtfs.shapes
            WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 3 OR route_type = 11 OR route_type = 200)
        ) as tile WHERE geom IS NOT NULL;
    
        RETURN mvt;
        END
        $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION notbus(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'notbus', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(linestring, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
            FROM gtfs.shapes
            WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND route_type != 3 AND route_type != 11
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION localrail(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'localrail', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(linestring, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
            FROM gtfs.shapes
            WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 0 OR route_type = 1 OR route_type = 5 OR route_type = 12)
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION intercityrail(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(linestring, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
            FROM gtfs.shapes
            WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 2)
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION other(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(linestring, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
            FROM gtfs.shapes
            WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 4 OR route_type = 6 OR route_type = 7)
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION stationfeatures(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'stationfeatures', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(point, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
            FROM gtfs.stops
            WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (location_type=2 OR location_type=3 OR location_type=4)
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION busstops(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'busstops', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(point, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id,  REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
            FROM gtfs.stops
            WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[3,11,200,1700,1500,1702]::smallint[] && route_types::smallint[] OR ARRAY[3,11,200,1700,1500,1702]::smallint[] && children_route_types::smallint[]) AND hidden = false
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION railstops(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'railstops', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(point, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
            FROM gtfs.stops
            WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]) AND hidden = false
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    
        client.batch_execute("
            CREATE OR REPLACE
            FUNCTION otherstops(z integer, x integer, y integer)
            RETURNS bytea AS $$
            DECLARE
            mvt bytea;
            BEGIN
            SELECT INTO mvt ST_AsMVT(tile, 'otherstops', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(point, 3857),
                ST_TileEnvelope(z, x, y),
                4096, 64, true) AS geom,
                onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
            FROM gtfs.stops
            WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[4,6,7]::smallint[] && route_types::smallint[] OR ARRAY[4,6,7]::smallint[] && children_route_types::smallint[])
            ) as tile WHERE geom IS NOT NULL;
        
            RETURN mvt;
            END
            $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
        ").await.unwrap();
    

}

async fn insertgtfs(client: &Client, gtfs: PathBuf) {
    let onestop_feed_id = gtfs.file_stem().unwrap().to_str().unwrap();
    let gtfs = Gtfs::from_path(gtfs.as_os_str());
    if gtfs.is_ok() {
        let gtfs = gtfs.unwrap();

        for agency in gtfs.agencies {
            client.execute("
                INSERT INTO agency (
                    agency_id,
                    agency_name,
                    agency_url,
                    agency_timezone,
                    agency_lang,
                    agency_phone,
                    agency_fare_url,
                    agency_email,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                ) ON CONFLICT (onestop_feed_id) 
                DO UPDATE SET
                    agency_name = EXCLUDED.agency_name,
                    agency_url = EXCLUDED.agency_url,
                    agency_timezone = EXCLUDED.agency_timezone,
                    agency_lang = EXCLUDED.agency_lang,
                    agency_phone = EXCLUDED.agency_phone,
                    agency_fare_url = EXCLUDED.agency_fare_url,
                    agency_email = EXCLUDED.agency_email;",
                &[
                    &agency.id,
                    &agency.name,
                    &agency.url,
                    &agency.timezone,
                    &agency.lang,
                    &agency.phone,
                    &agency.fare_url,
                    &agency.email,
                    &onestop_feed_id
                ]
            ).await.unwrap();
        }
        for calendar in gtfs.calendar  {
            client.execute("
                INSERT INTO calendar (
                    service_id,
                    monday,
                    tuesday,
                    wednesday,
                    thursday,
                    friday,
                    saturday,
                    sunday,
                    start_date,
                    end_date,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                ) ON CONFLICT (onestop_feed_id, service_id)
                DO UPDATE SET
                    service_id = excluded.service_id,
                    monday = excluded.monday,
                    tuesday = excluded.tuesday,
                    wednesday = excluded.wednesday,
                    thursday = excluded.thursday,
                    friday = excluded.friday,
                    saturday = excluded.saturday,
                    sunday = excluded.sunday,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date;",
                &[
                    &calendar.0, 
                    &calendar.1.monday, 
                    &calendar.1.tuesday, 
                    &calendar.1.wednesday, 
                    &calendar.1.thursday,
                    &calendar.1.friday, 
                    &calendar.1.saturday, 
                    &calendar.1.sunday,
                    &calendar.1.start_date,
                    &calendar.1.end_date, 
                    &onestop_feed_id
                ],
            ).await.unwrap();
        }
        for calendar_date in gtfs.calendar_dates {
            for date in calendar_date.1 {
                client.execute("
                    INSERT INTO calendar_dates (
                        service_id,
                        date,
                        exception_type,
                        onestop_feed_id
                    ) VALUES (
                        $1, $2, $3, $4
                    ) ON CONFLICT (onestop_feed_id, service_id) 
                    DO UPDATE SET 
                        date = EXCLUDED.date,
                        exception_type = EXCLUDED.exception_type;",
                    &[
                        &calendar_date.0,
                        &date.date,
                        &match date.exception_type {
                            Exception::Added => 1_i32,
                            Exception::Deleted => 2,
                        },
                        &onestop_feed_id
                    ]
                ).await.unwrap();
            }
        }
        for stop in gtfs.stops {
            client.execute("
                INSERT INTO stops (
                    stop_id,
                    stop_code,
                    stop_name,
                    tts_stop_name,
                    stop_desc,
                    stop_lat,
                    stop_lon,
                    zone_id,
                    stop_url,
                    location_type,
                    parent_station,
                    stop_timezone,
                    wheelchair_boarding,
                    level_id,
                    platform_code,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                ) ON CONFLICT (onestop_feed_id, stop_id) 
                DO UPDATE SET 
                    stop_code = EXCLUDED.stop_code,
                    stop_name = EXCLUDED.stop_name,
                    tts_stop_name = EXCLUDED.tts_stop_name,
                    stop_desc = EXCLUDED.stop_desc,
                    stop_lat = EXCLUDED.stop_lat,
                    stop_lon = EXCLUDED.stop_lon,
                    zone_id = EXCLUDED.zone_id,
                    stop_url = EXCLUDED.stop_url,
                    location_type = EXCLUDED.location_type,
                    parent_station = EXCLUDED.parent_station,
                    stop_timezone = EXCLUDED.stop_timezone,
                    wheelchair_boarding = EXCLUDED.wheelchair_boarding,
                    level_id = EXCLUDED.level_id,
                    platform_code = EXCLUDED.platform_code;",
                &[
                    &stop.0,
                    &stop.1.code.clone(),
                    &stop.1.name.clone(),
                    &stop.1.tts_name.clone(),
                    &stop.1.description.clone(),
                    &stop.1.latitude.clone(),
                    &stop.1.longitude.clone(),
                    &stop.1.zone_id.clone(),
                    &stop.1.url.clone(),
                    &match stop.1.location_type.clone() {
                        LocationType::StopPoint => 0,
                        LocationType::StopArea => 1,
                        LocationType::StationEntrance => 2,
                        LocationType::GenericNode => 3,
                        LocationType::BoardingArea => 4,
                        LocationType::Unknown(i) => i as i32,
                    },
                    &stop.1.parent_station.clone(),
                    &stop.1.timezone.clone(),
                    &match stop.1.wheelchair_boarding.clone() {
                        Availability::InformationNotAvailable => 0,
                        Availability::Available => 1,
                        Availability::NotAvailable => 2,
                        Availability::Unknown(i) => i as i32,
                    },
                    &stop.1.level_id.clone(),
                    &stop.1.platform_code.clone(),
                    &onestop_feed_id
                ]
            ).await.unwrap();
        }
        for route in gtfs.routes {
            client.execute("
                INSERT INTO routes (
                    route_id,
                    agency_id,
                    route_short_name,
                    route_long_name,
                    route_desc,
                    route_type,
                    route_url,
                    route_color,
                    route_text_color,
                    route_sort_order,
                    continuous_pickup,
                    continuous_drop_off,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                )",
                &[
                    &route.1.id,
                    &route.1.agency_id,
                    &route.1.short_name,
                    &route.1.long_name,
                    &route.1.desc,
                    &match route.1.route_type {
                        RouteType::Tramway => 0_i32,
                        RouteType::Subway => 1,
                        RouteType::Rail => 2,
                        RouteType::Bus => 3,
                        RouteType::Ferry => 4,
                        RouteType::CableCar => 5,
                        RouteType::Gondola => 6,
                        RouteType::Funicular => 7,
                        RouteType::Coach => 2,
                        RouteType::Air => 11,
                        RouteType::Taxi => 15,
                        RouteType::Other(i) => i as i32,
                    },
                    &route.1.url,
                    &route.1.color.to_string(),
                    &route.1.text_color.to_string(),
                    &route.1.order.map(|x| x as i32),
                    &match route.1.continuous_pickup {
                        ContinuousPickupDropOff::Continuous => 0_i32,
                        ContinuousPickupDropOff::NotAvailable => 1,
                        ContinuousPickupDropOff::ArrangeByPhone => 2,
                        ContinuousPickupDropOff::CoordinateWithDriver => 3,
                        ContinuousPickupDropOff::Unknown(i) => i as i32,
                    },
                    &match route.1.continuous_drop_off {
                        ContinuousPickupDropOff::Continuous => 0_i32,
                        ContinuousPickupDropOff::NotAvailable => 1,
                        ContinuousPickupDropOff::ArrangeByPhone => 2,
                        ContinuousPickupDropOff::CoordinateWithDriver => 3,
                        ContinuousPickupDropOff::Unknown(i) => i as i32,
                    },
                    &onestop_feed_id
                ]
            ).await.unwrap();
        }
        for trip in gtfs.trips {
            client.execute("
                INSERT INTO trips (
                    route_id,
                    service_id,
                    trip_id,
                    trip_headsign,
                    trip_short_name,
                    direction_id,
                    block_id,
                    shape_id,
                    wheelchair_accessible,
                    bikes_allowed,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                ) ON CONFLICT (onestop_feed_id, trip_id) 
                DO UPDATE SET
                    route_id = EXCLUDED.route_id,
                    service_id = EXCLUDED.service_id,
                    trip_id = EXCLUDED.trip_id,
                    trip_headsign = EXCLUDED.trip_headsign,
                    trip_short_name = EXCLUDED.trip_short_name,
                    direction_id = EXCLUDED.direction_id,
                    block_id = EXCLUDED.block_id,
                    shape_id = EXCLUDED.shape_id,
                    wheelchair_accessible = EXCLUDED.wheelchair_accessible,
                    bikes_allowed = EXCLUDED.bikes_allowed;",
                &[
                    &trip.1.route_id,
                    &trip.1.service_id,
                    &trip.0,
                    &trip.1.trip_headsign,
                    &trip.1.trip_short_name,
                    &match trip.1.direction_id {
                        Some(DirectionType::Outbound) => Some(0_i32),
                        Some(DirectionType::Inbound) => Some(1),
                        None => None,
                    },
                    &trip.1.block_id,
                    &trip.1.shape_id,
                    &match trip.1.wheelchair_accessible {
                        Availability::InformationNotAvailable => 0,
                        Availability::Available => 1,
                        Availability::NotAvailable => 2,
                        Availability::Unknown(i) => i as i32,

                    },
                    &match trip.1.bikes_allowed {
                        BikesAllowedType::NoBikeInfo => 0,
                        BikesAllowedType::AtLeastOneBike => 1,
                        BikesAllowedType::NoBikesAllowed => 2,
                        BikesAllowedType::Unknown(i) => i as i32,
                    },
                    &onestop_feed_id
                ]
            ).await.unwrap();
        }
        let mut features = Vec::new();
        for shapes in gtfs.shapes {
            let mut current_shape = shapes.1.first().unwrap().id.to_owned();
            let mut shape_vec = Vec::new();
            for shape in shapes.1 {
                if current_shape.to_owned() != shape.id {
                    features.push((current_shape, GeoJsonFeatureCollection {
                        type_: "FeatureCollection".to_string(),
                        features: shape_vec,
                    }));
                    shape_vec = Vec::new();
                    
                    current_shape = shape.id.to_owned();
                }
                let point = GeoJsonPoint {
                    type_: "Point".to_string(),
                    coordinates: [shape.longitude, shape.latitude],
                    properties: GeoJsonProperties {
                        sequence: shape.sequence,
                        dist_traveled: shape.dist_traveled,
                    },
                };
                shape_vec.push(json!(point));
            }

        }
        for feature in &features {
            client.execute("
                INSERT INTO shapes (
                    shape_id,
                    shape_geojson,
                    exception_type,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4
                ) ON CONFLICT (onestop_feed_id, shape_id) 
                DO UPDATE SET
                    shape_geojson = EXCLUDED.shape_geojson,
                    exception_type = EXCLUDED.exception_type;",
                &[
                    &feature.0,
                    &serde_json::to_string(&feature.1).unwrap(),
                    &onestop_feed_id
                ],
            ).await.unwrap();
        }
        std::mem::drop(features);
        for fare_attribute in gtfs.fare_attributes {
            client.execute("
                INSERT INTO fare_attributes (
                    fare_id,
                    price,
                    currency_type,
                    payment_method,
                    transfers,
                    agency_id,
                    transfer_duration,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8
                ) ON CONFLICT (onestop_feed_id, fare_id) 
                DO UPDATE SET
                    price = EXCLUDED.price,
                    currency_type = EXCLUDED.currency_type,
                    payment_method = EXCLUDED.payment_method,
                    transfers = EXCLUDED.transfers,
                    agency_id = EXCLUDED.agency_id,
                    transfer_duration = EXCLUDED.transfer_duration;",
                &[
                    &fare_attribute.0,
                    &fare_attribute.1.price,
                    &fare_attribute.1.currency,
                    &match fare_attribute.1.payment_method {
                        PaymentMethod::Aboard => "0",
                        PaymentMethod::PreBoarding => "1",
                    },
                    &match fare_attribute.1.transfers {
                        Transfers::Unlimited => i16::MAX,
                        Transfers::NoTransfer => 0,
                        Transfers::UniqueTransfer => 1,
                        Transfers::TwoTransfers => 2,
                        Transfers::Other(i16) => i16,
                    },
                    &fare_attribute.1.agency_id,
                    &fare_attribute.1.transfer_duration.map(|x| x as i32),
                    &onestop_feed_id
                ],
            ).await.unwrap();
        }
        for fare_rule in gtfs.fare_rules {
            for rule in fare_rule.1 {
                client.execute("
                    INSERT INTO fare_rules (
                        fare_id,
                        route_id,
                        origin_id,
                        destination_id,
                        contains_id,
                        onestop_feed_id
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6
                    ) ON CONFLICT (onestop_feed_id, fare_id) 
                    DO UPDATE SET
                        fare_id = EXCLUDED.fare_id,
                        route_id = EXCLUDED.route_id,
                        origin_id = EXCLUDED.origin_id,
                        destination_id = EXCLUDED.destination_id,
                        contains_id = EXCLUDED.contains_id;",
                    &[
                        &fare_rule.0,
                        &rule.route_id.unwrap(),
                        &rule.origin_id.unwrap(),
                        &rule.destination_id.unwrap(),
                        &rule.contains_id.unwrap(),
                        &onestop_feed_id
                    ],
                ).await.unwrap();
            }
        }
        for feed_info in gtfs.feed_info {
            client.execute("
                INSERT INTO feed_info (
                    feed_publisher_name,
                    feed_publisher_url,
                    feed_lang,
                    feed_start_date,
                    feed_end_date,
                    feed_version,
                    feed_contact_email,
                    feed_contact_url,
                    default_lang,
                    onestop_feed_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                ) ON CONFLICT (onestop_feed_id) 
                DO UPDATE SET
                    feed_publisher_name = EXCLUDED.feed_publisher_name,
                    feed_publisher_url = EXCLUDED.feed_publisher_url,
                    feed_lang = EXCLUDED.feed_lang,
                    feed_start_date = EXCLUDED.feed_start_date,
                    feed_end_date = EXCLUDED.feed_end_date,
                    feed_version = EXCLUDED.feed_version,
                    feed_contact_email = EXCLUDED.feed_contact_email,
                    feed_contact_url = EXCLUDED.feed_contact_url,
                    default_lang = EXCLUDED.default_lang;",
                &[
                    &feed_info.name,
                    &feed_info.url,
                    &feed_info.lang,
                    &feed_info.start_date,
                    &feed_info.end_date,
                    &feed_info.version,
                    &feed_info.contact_email,
                    &feed_info.contact_url,
                    &feed_info.default_lang,
                    &onestop_feed_id
                ]
            ).await.unwrap();
        }
    }
}

#[tokio::main]
async fn main() {
    let gtfs_dir = arguments::parse(std::env::args()).unwrap().get::<String>("dir").unwrap_or("./gtfs/".to_string());

    let conn_string = "postgresql://postgres:password@localhost/postgres";
    let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    makedb(&client).await;
    drop(client);
    let mut futs = FuturesUnordered::new();
    let mut outputs = Vec::new();
    if let Ok(entries) = fs::read_dir(gtfs_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();            
                if path.is_file() {
                    if let Some(file_name) = path.file_stem() {
                        if let Some(file) = file_name.to_str().clone() {
                            println!("Starting, {}", file);
                            let file = file.to_string().clone();
                            let fut = async move {
                                let conn_string = "postgresql://postgres:password@localhost/postgres";
                                let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await.unwrap();

                                tokio::spawn(async move {
                                    if let Err(e) = connection.await {
                                        eprintln!("connection error: {}", e);
                                    }
                                });
                                insertgtfs(&client, path).await;
                                println!("Finished: {}", file);
                            };
                            futs.push(task::spawn(fut));
                            if futs.len() == 128 {
                                futs.next().await;
                                outputs.push(());
                            }
                        }
                    }
                }
            }
        }
    } else {
        eprintln!("Error reading the directory");
    }
    while let Some(_item) = futs.next().await {
        outputs.push(());
    }
}
