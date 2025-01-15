use actix_web::middleware::DefaultHeaders;

pub fn parse_rgb_string(color: &str) -> Result<Rgb<u8>, String> {
    if color.starts_with("rgb(") && color.ends_with(")") {
        let inner = &color[4..color.len() - 1];
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        if parts.len() == 3 {
            let r = parts[0].parse::<u8>().map_err(|_| "Invalid red component")?;
            let g = parts[1].parse::<u8>().map_err(|_| "Invalid green component")?;
            let b = parts[2].parse::<u8>().map_err(|_| "Invalid blue component")?;
            return Ok(Rgb::new(r, g, b));
        }
    }
    Err("Invalid color format. Expected format: rgb(r,g,b)".to_string())
}

mod errors {
    use actix_web::{HttpResponse, ResponseError};
    use deadpool_postgres::PoolError;
    use derive_more::{Display, From};
    use tokio_pg_mapper::Error as PGMError;
    use tokio_postgres::error::Error as PGError;

    #[derive(Display, From, Debug)]
    pub enum MyError {
        NotFound,
        WrongType,
        PGError(PGError),
        PGMError(PGMError),
        PoolError(PoolError),
    }
    impl std::error::Error for MyError {}

    impl ResponseError for MyError {
        fn error_response(&self) -> HttpResponse {
            match *self {
                MyError::NotFound => HttpResponse::NotFound().finish(),
                MyError::PoolError(ref err) => {
                    HttpResponse::InternalServerError().body(err.to_string())
                }
                _ => HttpResponse::InternalServerError().finish(),
            }
        }
    }
}

mod db {
    use deadpool_postgres::Client;
    use gtfs_structures::{Agency, Availability, ContinuousPickupDropOff, LocationType, Route, RouteType, Stop};
    use qstring::QString;

    use crate::{errors::MyError, parse_rgb_string};

    pub async fn agency(client: &Client, onestop_feed_id: String, qs: QString) -> Result<Vec<Agency>, MyError> {
        let stmt = "SELECT * 
        FROM gtfs.agency 
        WHERE onestop_feed_id = $1
            AND agency_id LIKE $2
            AND agency_name LIKE $3
            AND agency_url LIKE $4
            AND agency_timezone LIKE $5
            AND agency_lang LIKE $6
            AND agency_phone LIKE $7
            AND agency_fare_url LIKE $8
            AND agency_email LIKE $9";
        let results = client
            .query(stmt,&[
                &onestop_feed_id, 
                &qs.get("agency_id").unwrap_or("%"),
                &qs.get("agency_name").unwrap_or("%"),
                &qs.get("agency_url").unwrap_or("%"),
                &qs.get("agency_timezone").unwrap_or("%"),
                &qs.get("agency_lang").unwrap_or("%"),
                &qs.get("agency_phone").unwrap_or("%"),
                &qs.get("agency_fare_url").unwrap_or("%"),
                &qs.get("agency_email").unwrap_or("%")
            ])
            .await?
            .iter()
            .map(|row| Agency {
                id: row.get("agency_id"),
                name: row.get("agency_name"),
                url: row.get("agency_url"),
                timezone: row.get("agency_timezone"),
                lang: row.get("agency_lang"),
                phone: row.get("agency_phone"),
                fare_url: row.get("agency_fare_url"),
                email: row.get("agency_email"),
            })
            .collect::<Vec<Agency>>();
        
        Ok(results)
    }

    pub async fn stops(client: &Client, onestop_feed_id: String, qs: QString) -> Result<Vec<Stop>, MyError> {
        let stmt = "SELECT * 
        FROM gtfs.stops 
        WHERE onestop_feed_id = $1
            AND stop_id LIKE $2
            AND stop_code LIKE $3
            AND stop_name LIKE $4
            AND tts_stop_name LIKE $5
            AND stop_desc LIKE $6
            AND stop_lat LIKE $7
            AND stop_lon LIKE $8
            AND zone_id LIKE $9
            AND stop_url LIKE $10
            AND location_type LIKE $11
            AND parent_station LIKE $12
            AND stop_timezone LIKE $13
            AND wheelchair_boarding LIKE $14
            AND level_id LIKE $15
            AND platform_code LIKE $16";
        let results = client
            .query(stmt,&[
                &onestop_feed_id,
                &qs.get("stop_id").unwrap_or("%"),
                &qs.get("stop_code").unwrap_or("%"),
                &qs.get("stop_name").unwrap_or("%"),
                &qs.get("tts_stop_name").unwrap_or("%"),
                &qs.get("stop_desc").unwrap_or("%"),
                &qs.get("stop_lat").unwrap_or("%"),
                &qs.get("stop_lon").unwrap_or("%"),
                &qs.get("zone_id").unwrap_or("%"),
                &qs.get("stop_url").unwrap_or("%"),
                &qs.get("location_type").unwrap_or("%"),
                &qs.get("parent_station").unwrap_or("%"),
                &qs.get("stop_timezone").unwrap_or("%"),
                &qs.get("wheelchair_boarding").unwrap_or("%"),
                &qs.get("level_id").unwrap_or("%"),
                &qs.get("platform_code").unwrap_or("%")
            ])
            .await?
            .iter()
            .map(|row| Stop {
                id: row.get("stop_id"),
                code: row.get("stop_code"),
                name: row.get("stop_name"),
                description: row.get("stop_desc"),
                location_type: match row.get("location_type") {
                    0 => LocationType::StopPoint,
                    1 => LocationType::StopArea,
                    2 => LocationType::StationEntrance,
                    3 => LocationType::GenericNode,
                    4 => LocationType::BoardingArea,
                    other => LocationType::Unknown(other)
                },
                parent_station: row.get("parent_station"),
                zone_id: row.get("zone_id"),
                url: row.get("stop_url"),
                longitude: row.get("stop_lat"),
                latitude: row.get("stop_lon"),
                timezone: row.get("stop_timezone"),
                wheelchair_boarding: match row.get("wheelchair_boarding") {
                    0 => Availability::InformationNotAvailable,
                    1 => Availability::Available,
                    2 => Availability::NotAvailable,
                    other => Availability::Unknown(other)
                },
                level_id: row.get("level_id"),
                platform_code: row.get("platform_code"),
                transfers: vec![],
                pathways: vec![],
                tts_name: row.get("tts_stop_name"),
            })
            .collect::<Vec<Stop>>();
        
        Ok(results)
    }

    pub async fn routes(client: &Client, onestop_feed_id: String, qs: QString) -> Result<Vec<Route>, MyError> {
        let stmt = "SELECT * 
        FROM gtfs.routes 
        WHERE onestop_feed_id = $1
            AND route_id LIKE $2
            AND agency_id LIKE $3
            AND route_short_name LIKE $4
            AND route_long_name LIKE $5
            AND route_desc LIKE $6
            AND route_type LIKE $7
            AND route_url LIKE $8
            AND route_color LIKE $9
            AND route_text_color LIKE $10
            AND route_sort_order LIKE $11
            AND continuous_pickup LIKE $12
            AND continuous_drop_off LIKE $13";
        let results = client
            .query(stmt,&[
                &onestop_feed_id,
                &qs.get("route_id").unwrap_or("%"),
                &qs.get("agency_id").unwrap_or("%"),
                &qs.get("route_short_name").unwrap_or("%"),
                &qs.get("route_long_name").unwrap_or("%"),
                &qs.get("route_desc").unwrap_or("%"),
                &qs.get("route_type").unwrap_or("%"),
                &qs.get("route_url").unwrap_or("%"),
                &qs.get("route_color").unwrap_or("%"),
                &qs.get("route_text_color").unwrap_or("%"),
                &qs.get("route_sort_order").unwrap_or("%"),
                &qs.get("continuous_pickup").unwrap_or("%"),
                &qs.get("continuous_drop_off").unwrap_or("%")
            ])
            .await?
            .iter()
            .map(|row| Route {
                id: row.get("route_id"),
                short_name: row.get("route_short_name"),
                long_name: row.get("route_long_name"),
                desc: row.get("route_desc"),
                route_type: match row.get("route_type") {
                    0 => RouteType::Tramway,
                    1 => RouteType::Subway,
                    2 => RouteType::Rail,
                    3 => RouteType::Bus,
                    4 => RouteType::Ferry,
                    5 => RouteType::CableCar,
                    6 => RouteType::Gondola,
                    7 => RouteType::Funicular,
                    11 => RouteType::Air,
                    15 => RouteType::Taxi,
                    other => RouteType::Other(other),
                },
                url: row.get("route_url"),
                agency_id: row.get("agency_id"),
                order: row.get("route_sort_order"),
                color: parse_rgb_string(row.get("route_color")).unwrap(),
                text_color: parse_rgb_string(row.get("route_text_color")).unwrap(),
                continuous_pickup: match row.get("continuous_pickup") {
                    0 => ContinuousPickupDropOff::Continuous,
                    1 => ContinuousPickupDropOff::NotAvailable,
                    2 => ContinuousPickupDropOff::ArrangeByPhone,
                    3 => ContinuousPickupDropOff::CoordinateWithDriver,
                    other => ContinuousPickupDropOff::Unknown(other),
                },
                continuous_drop_off: match row.get("continuous_drop_off") {
                    0 => ContinuousPickupDropOff::Continuous,
                    1 => ContinuousPickupDropOff::NotAvailable,
                    2 => ContinuousPickupDropOff::ArrangeByPhone,
                    3 => ContinuousPickupDropOff::CoordinateWithDriver,
                    other => ContinuousPickupDropOff::Unknown(other),
                },
            })
            .collect::<Vec<Route>>();
        
        Ok(results)
    }
}

mod handlers {
    use actix_web::{web, HttpRequest, HttpResponse, Responder};
    use deadpool_postgres::{Client, Pool};
    use qstring::QString;
    use crate::{db, errors::MyError};

    pub async fn index() -> impl Responder {
        HttpResponse::Ok().body("Ok")
    }
    pub async fn agency(path: web::Path<String>, db_pool: web::Data<Pool>, req: HttpRequest) -> impl Responder {
        let qs = QString::from(req.query_string());
        let onestop_feed_id = path.into_inner();
        let client: Client = db_pool.get().await.map_err(MyError::PoolError).unwrap();
        match db::agency(&client, onestop_feed_id.clone(), qs).await {
            Ok(res) => HttpResponse::Ok().json(res),
            Err(_) => HttpResponse::NotFound().body(format!("{} feed_id not found", onestop_feed_id)).into(),
        }
    }

    pub async fn stops(path: web::Path<String>, db_pool: web::Data<Pool>, req: HttpRequest) -> impl Responder {
        let qs = QString::from(req.query_string());
        let onestop_feed_id = path.into_inner();
        let client: Client = db_pool.get().await.map_err(MyError::PoolError).unwrap();
        match db::stops(&client, onestop_feed_id.clone(), qs).await {
            Ok(res) => HttpResponse::Ok().json(res),
            Err(_) => HttpResponse::NotFound().body(format!("{} feed_id not found", onestop_feed_id)).into(),
        }
    }

    pub async fn routes(path: web::Path<String>, db_pool: web::Data<Pool>, req: HttpRequest) -> impl Responder {
        let qs = QString::from(req.query_string());
        let onestop_feed_id = path.into_inner();
        let client: Client = db_pool.get().await.map_err(MyError::PoolError).unwrap();
        match db::routes(&client, onestop_feed_id.clone(), qs).await {
            Ok(res) => HttpResponse::Ok().json(res),
            Err(_) => HttpResponse::NotFound().body(format!("{} feed_id not found", onestop_feed_id)).into(),
        }
    }
}

use actix_web::{web, App, HttpServer};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rgb::Rgb;
use tokio_postgres::NoTls;
use handlers::{agency, index, routes, stops};

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let mut pg_config = tokio_postgres::Config::new();
    //postgresql://postgres:password@localhost/postgres
    pg_config.user("postgres");
    pg_config.password("password");
    pg_config.host("localhost");
    pg_config.dbname("postgres");
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast
    };
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(16).build().unwrap();

    let server = HttpServer::new(move || {
        App::new()
        .wrap(
            DefaultHeaders::new()
        .add((
            "Access-Control-Allow-Origin",
            "*",
        ))
        )
        .app_data(web::Data::new(pool.clone())).service(web::resource("/").route(web::get().to(index)))
        .service(web::resource("/gtfs/{onestop_feed_id}/agency/").route(web::get().to(agency)))
        .service(web::resource("/gtfs/{onestop_feed_id}/agency").route(web::get().to(agency)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stops/").route(web::get().to(stops)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stops").route(web::get().to(stops)))
        .service(web::resource("/gtfs/{onestop_feed_id}/routes/").route(web::get().to(routes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/routes").route(web::get().to(routes)))
        /*
        .service(web::resource("/gtfs/{onestop_feed_id}/trips/").route(web::get().to(trips)))
        .service(web::resource("/gtfs/{onestop_feed_id}/trips").route(web::get().to(trips)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stop_times/").route(web::get().to(stop_times)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stop_times").route(web::get().to(stop_times)))
        .service(web::resource("/gtfs/{onestop_feed_id}/attributions/").route(web::get().to(attributions)))
        .service(web::resource("/gtfs/{onestop_feed_id}/attributions").route(web::get().to(attributions)))
        .service(web::resource("/gtfs/{onestop_feed_id}/calendar/").route(web::get().to(calendar)))
        .service(web::resource("/gtfs/{onestop_feed_id}/calendar").route(web::get().to(calendar)))
        .service(web::resource("/gtfs/{onestop_feed_id}/calendar_dates/").route(web::get().to(calendar_dates)))
        .service(web::resource("/gtfs/{onestop_feed_id}/calendar_dates").route(web::get().to(calendar_dates)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_attributes/").route(web::get().to(fare_attributes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_attributes").route(web::get().to(fare_attributes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_rules/").route(web::get().to(fare_rules)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_rules").route(web::get().to(fare_rules)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_media/").route(web::get().to(fare_media)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_media").route(web::get().to(fare_media)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_products/").route(web::get().to(fare_products)))
        .service(web::resource("/gtfs/{onestop_feed_id}/fare_products").route(web::get().to(fare_products)))
        .service(web::resource("/gtfs/{onestop_feed_id}/areas/").route(web::get().to(areas)))
        .service(web::resource("/gtfs/{onestop_feed_id}/areas").route(web::get().to(areas)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stop_areas/").route(web::get().to(stop_areas)))
        .service(web::resource("/gtfs/{onestop_feed_id}/stop_areas").route(web::get().to(stop_areas)))
        .service(web::resource("/gtfs/{onestop_feed_id}/networks/").route(web::get().to(networks)))
        .service(web::resource("/gtfs/{onestop_feed_id}/networks").route(web::get().to(networks)))
        .service(web::resource("/gtfs/{onestop_feed_id}/route_networks/").route(web::get().to(route_networks)))
        .service(web::resource("/gtfs/{onestop_feed_id}/route_networks").route(web::get().to(route_networks)))
        .service(web::resource("/gtfs/{onestop_feed_id}/shapes/").route(web::get().to(shapes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/shapes").route(web::get().to(shapes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/frequencies/").route(web::get().to(frequencies)))
        .service(web::resource("/gtfs/{onestop_feed_id}/frequencies").route(web::get().to(frequencies)))
        .service(web::resource("/gtfs/{onestop_feed_id}/timeframes/").route(web::get().to(timeframes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/timeframes").route(web::get().to(timeframes)))
        .service(web::resource("/gtfs/{onestop_feed_id}/transfers/").route(web::get().to(transfers)))
        .service(web::resource("/gtfs/{onestop_feed_id}/transfers").route(web::get().to(transfers)))
        .service(web::resource("/gtfs/{onestop_feed_id}/pathways/").route(web::get().to(pathways)))
        .service(web::resource("/gtfs/{onestop_feed_id}/pathways").route(web::get().to(pathways)))
        .service(web::resource("/gtfs/{onestop_feed_id}/levels/").route(web::get().to(levels)))
        .service(web::resource("/gtfs/{onestop_feed_id}/levels").route(web::get().to(levels)))
        .service(web::resource("/gtfs/{onestop_feed_id}/feed_info/").route(web::get().to(feed_info)))
        .service(web::resource("/gtfs/{onestop_feed_id}/feed_info").route(web::get().to(feed_info)))
        .service(web::resource("/gtfs/{onestop_feed_id}/translations/").route(web::get().to(translations)))
        .service(web::resource("/gtfs/{onestop_feed_id}/translations").route(web::get().to(translations)))
        */
    })
    .bind("127.0.0.1:16969")?
    .run();
    println!("Server running at http://127.0.0.1:16969/");

    server.await
}