use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    let conn_string = "postgresql://postgres:password@localhost/postgres";
    let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client.batch_execute("
        CREATE OR REPLACE FUNCTION simplification_tolerance(z integer)
        RETURNS float AS $$
        BEGIN
            -- Coarser simplification at lower zooms
            RETURN CASE
                WHEN z <= 6 THEN 0.01
                WHEN z <= 9 THEN 0.001
                WHEN z <= 12 THEN 0.0001
                ELSE 0.00001
            END;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT;
    ").await.unwrap();
    client.batch_execute("
        CREATE OR REPLACE FUNCTION gtfs.shapes(z integer, x integer, y integer)
        RETURNS bytea AS $$
        DECLARE
            mvt bytea;
            tile_envelope geometry;
        BEGIN
            IF x < 0 OR x >= (1 << z) OR y < 0 OR y >= (1 << z) THEN
                RETURN NULL;
            END IF;

            tile_envelope := ST_TileEnvelope(z, x, y);

            SELECT INTO mvt ST_AsMVT(tile, 'shapes', 4096, 'geom')
            FROM (
                SELECT
                    ST_AsMVTGeom(
                        ST_Transform(
                            ST_SimplifyPreserveTopology(s.shape_linestring, simplification_tolerance(z)),
                            3857
                        ),
                        tile_envelope,
                        4096, 64, true
                    ) AS geom,
                    s.onestop_feed_id,
                    s.shape_id,
                    r.route_color
                FROM gtfs.shapes s
                JOIN gtfs.trips t ON s.shape_id = t.shape_id
                JOIN gtfs.routes r ON t.route_id = r.route_id
                WHERE s.shape_linestring && ST_Transform(tile_envelope, 4326)
            ) AS tile
            WHERE geom IS NOT NULL;

            RETURN mvt;
        END
        $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").await.unwrap();

    client.batch_execute("
        CREATE OR REPLACE
        FUNCTION gtfs.busonly(z integer, x integer, y integer)
        RETURNS bytea AS $$
        DECLARE
        mvt bytea;
        BEGIN
        SELECT INTO mvt ST_AsMVT(tile, 'busonly', 4096, 'geom') FROM (
            SELECT
            ST_AsMVTGeom(
                ST_Transform(shape_geojson, 3857),
                            ST_TileEnvelope(z, x, y),
                            4096, 64, true) AS geom,
                            onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
                            FROM gtfs.shapes
                            WHERE (shape_geojson && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 3 OR route_type = 11 OR route_type = 200)
        ) as tile WHERE geom IS NOT NULL;

        RETURN mvt;
        END
        $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").await.unwrap();

    client.batch_execute("
        CREATE OR REPLACE
        FUNCTION gtfs.notbus(z integer, x integer, y integer)
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
    FUNCTION gtfs.localrail(z integer, x integer, y integer)
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
    FUNCTION gtfs.intercityrail(z integer, x integer, y integer)
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
    FUNCTION gtfs.other(z integer, x integer, y integer)
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
    FUNCTION gtfs.stationfeatures(z integer, x integer, y integer)
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
    FUNCTION gtfs.busstops(z integer, x integer, y integer)
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
    FUNCTION gtfs.railstops(z integer, x integer, y integer)
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
    FUNCTION gtfs.otherstops(z integer, x integer, y integer)
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
