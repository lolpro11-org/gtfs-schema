use actix_web::middleware::DefaultHeaders;

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
    use gtfs_structures::Agency;

    use crate::errors::MyError;

    pub async fn agency(client: &Client, onestop_feed_id: String) -> Result<Vec<Agency>, MyError> {
        let stmt = "SELECT * 
        FROM gtfs.agency 
        WHERE onestop_feed_id LIKE $1";
        let results = client
            .query(stmt,&[&onestop_feed_id])
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
}

mod handlers {
    use actix_web::{web, HttpRequest, HttpResponse, Responder};
    use deadpool_postgres::{Client, Pool};
    use qstring::QString;
    use crate::{db, errors::MyError};

    pub async fn index() -> impl Responder {
        HttpResponse::Ok().body("Ok")
    }
    pub async fn agency(path: web::Path<String>, db_pool: web::Data<Pool>, _req: HttpRequest) -> impl Responder {
        let onestop_feed_id = path.into_inner();
        let client: Client = db_pool.get().await.map_err(MyError::PoolError).unwrap();
        match db::agency(&client, onestop_feed_id.clone()).await {
            Ok(res) => HttpResponse::Ok().json(res),
            Err(_) => HttpResponse::NotFound().body(format!("{} feed_id not found", onestop_feed_id)).into(),
        }
    }
}

use actix_web::{web, App, HttpServer};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;
use handlers::{index, agency};

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host_path("/run/postgresql");
    pg_config.host_path("/tmp");
    pg_config.user("postgres");
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
    })
    .bind("127.0.0.1:16969")?
    .run();
    println!("Server running at http://127.0.0.1:16969/");

    server.await
}