use std::fs;
mod dmfr;
use gtfs_structures::Gtfs;
use tokio_postgres::{Client, NoTls};

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
            agency_id text UNIQUE NULL,
            agency_name text NOT NULL,
            agency_url text NOT NULL,
            agency_timezone text NOT NULL,
            agency_lang text NULL,
            agency_phone text NULL,
            agency_fare_url text NULL,
            agency_email text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE levels (
            level_id text PRIMARY KEY,
            level_index double precision NOT NULL,
            level_name text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stops (
            stop_id text PRIMARY KEY,
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
            platform_code text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE routes (
            route_id text PRIMARY KEY,
            agency_id text NULL REFERENCES agency(agency_id) ON DELETE CASCADE ON UPDATE CASCADE,
            route_short_name text NULL,
            route_long_name text NULL CHECK (route_short_name IS NOT NULL OR route_long_name IS NOT NULL),
            route_desc text NULL,
            route_type integer NOT NULL,
            route_url text NULL,
            route_color text NULL,
            route_text_color text NULL,
            route_sort_order integer NULL CHECK (route_sort_order >= 0),
            continuous_pickup integer NULL,
            continuous_drop_off integer NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE trips (
            route_id text NOT NULL REFERENCES routes ON DELETE CASCADE ON UPDATE CASCADE,
            service_id text NOT NULL,
            trip_id text NOT NULL PRIMARY KEY,
            trip_headsign text NULL,
            trip_short_name text NULL,
            direction_id boolean NULL,
            block_id text NULL,
            shape_id text NULL,
            wheelchair_accessible integer NULL CHECK (wheelchair_accessible >= 0 AND wheelchair_accessible <= 2),
            bikes_allowed integer NULL CHECK (bikes_allowed >= 0 AND bikes_allowed <= 2)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stop_times (
            trip_id text NOT NULL REFERENCES trips ON DELETE CASCADE ON UPDATE CASCADE,
            arrival_time interval NULL,
            departure_time interval NOT NULL,
            stop_id text NOT NULL REFERENCES stops ON DELETE CASCADE ON UPDATE CASCADE,
            stop_sequence integer NOT NULL CHECK (stop_sequence >= 0),
            stop_headsign text NULL,
            pickup_type integer NOT NULL CHECK (pickup_type >= 0 AND pickup_type <= 3),
            drop_off_type integer NOT NULL CHECK (drop_off_type >= 0 AND drop_off_type <= 3),
            continuous_pickup integer NULL,
            continuous_drop_off integer NULL,
            shape_dist_traveled double precision NULL CHECK (shape_dist_traveled >= 0.0),
            timepoint boolean NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE calendar (
            service_id text PRIMARY KEY,
            monday boolean NOT NULL,
            tuesday boolean NOT NULL,
            wednesday boolean NOT NULL,
            thursday boolean NOT NULL,
            friday boolean NOT NULL,
            saturday boolean NOT NULL,
            sunday boolean NOT NULL,
            start_date numeric(8) NOT NULL,
            end_date numeric(8) NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE calendar_dates (
            service_id text NOT NULL,
            date numeric(8) NOT NULL,
            exception_type integer NOT NULL CHECK (exception_type >= 1 AND exception_type <= 2)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_attributes (
            fare_id text PRIMARY KEY,
            price double precision NOT NULL CHECK (price >= 0.0),
            currency_type text NOT NULL,
            payment_method boolean NOT NULL,
            transfers integer NULL CHECK (transfers >= 0 AND transfers <= 5),
            agency_id text NULL REFERENCES agency(agency_id) ON DELETE CASCADE ON UPDATE CASCADE,
            transfer_duration integer NULL CHECK (transfer_duration >= 0)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_rules (
            fare_id text NOT NULL REFERENCES fare_attributes ON DELETE CASCADE ON UPDATE CASCADE,
            route_id text NULL REFERENCES routes ON DELETE CASCADE ON UPDATE CASCADE,
            origin_id text NULL,
            destination_id text NULL,
            contains_id text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE timeframes (
            timeframe_group_id text NOT NULL,
            start_time interval NULL,
            end_time interval NULL,
            service_id text NOT NULL REFERENCES calendar ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_media (
            fare_media_id text PRIMARY KEY,
            fare_media_name text NULL,
            fare_media_type integer NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE fare_products (
            fare_product_id text PRIMARY KEY,
            fare_product_name text NULL,
            fare_media_id text REFERENCES fare_media ON DELETE CASCADE ON UPDATE CASCADE,
            amount text NOT NULL,
            currency text NOT NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE areas (
            area_id text PRIMARY KEY,
            area_name text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE stop_areas (
            area_id text NOT NULL REFERENCES areas ON DELETE CASCADE ON UPDATE CASCADE,
            stop_id text NOT NULL REFERENCES stops ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE networks (
            network_id text PRIMARY KEY,
            network_name text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE route_networks (
            network_id text NOT NULL REFERENCES networks ON DELETE CASCADE ON UPDATE CASCADE,
            network_name text NOT NULL REFERENCES routes ON DELETE CASCADE ON UPDATE CASCADE
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE shapes (
            shape_id text NOT NULL,
            shape_pt_lat double precision NOT NULL,
            shape_pt_lon double precision NOT NULL,
            shape_pt_sequence integer NOT NULL CHECK (shape_pt_sequence >= 0),
            shape_dist_traveled double precision NULL CHECK (shape_dist_traveled >= 0.0)
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE frequencies (
            trip_id text NOT NULL REFERENCES trips ON DELETE CASCADE ON UPDATE CASCADE,
            start_time interval NOT NULL,
            end_time interval NOT NULL,
            headway_secs integer NOT NULL CHECK (headway_secs >= 0),
            exact_times boolean NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE transfers (
            from_stop_id text NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            to_stop_id text NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            transfer_type integer NOT NULL CHECK (transfer_type >= 0 AND transfer_type <= 3),
            min_transfer_time integer NULL CHECK (min_transfer_time >= 0),
            from_route_id text NULL,
            to_route_id text NULL,
            from_trip_id text NULL,
            to_trip_id text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE pathways (
            pathway_id text PRIMARY KEY,
            from_stop_id text NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            to_stop_id text NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            pathway_mode integer NOT NULL CHECK (pathway_mode >= 1 AND pathway_mode <= 7),
            is_bidirectional boolean NOT NULL,
            length double precision NULL CHECK (length >= 0.0),
            traversal_time integer NULL CHECK (traversal_time >= 0),
            stair_count integer NULL,
            max_slope double precision NULL,
            min_width double precision NULL CHECK (min_width >= 0.0),
            signposted_as text NULL,
            reversed_signposted_as text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE feed_info (
            feed_publisher_name text NOT NULL,
            feed_publisher_url text NOT NULL,
            feed_lang text NOT NULL,
            feed_start_date numeric(8) NULL,
            feed_end_date numeric(8) NULL,
            feed_version text NULL,
            feed_contact_email text NULL,
            feed_contact_url text NULL,
            default_lang text NULL
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
            field_value text NULL
        );
    ").await.unwrap();
    client.batch_execute("
        CREATE TABLE attributions (
            attribution_id text PRIMARY KEY,
            agency_id text NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE ON UPDATE CASCADE,
            route_id text NULL REFERENCES routes(route_id) ON DELETE CASCADE ON UPDATE CASCADE,
            trip_id text NULL REFERENCES trips(trip_id) ON DELETE CASCADE ON UPDATE CASCADE,
            organization_name text NOT NULL,
            is_producer integer NULL,
            is_operator integer NULL,
            is_authority integer NULL,
            attribution_url text NULL,
            attribution_phone text NULL,
            attribution_email text NULL
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

async fn insertgtfs(client: &Client, gtfs: Gtfs) {

}

#[tokio::main]
async fn main() {
    let gtfs_dir = arguments::parse(std::env::args()).unwrap().get::<String>("dir").unwrap_or("/home/lolpro11/Documents/Catenary/catenary-backend/gtfs_static_zips/".to_string());

    let conn_string = "postgresql://lolpro11:lolpro11@localhost/catenary";
    let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    makedb(&client).await;
    if let Ok(entries) = fs::read_dir(gtfs_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                
                if path.is_file() {
                    if let Some(file_name) = path.file_stem() {
                        if let Some(file_name_str) = file_name.to_str() {
                            println!("{}", file_name_str);
                        }
                    }
                }
            }
        }
    } else {
        eprintln!("Error reading the directory");
    }
    
}
