mod dmfr;
use dmfr::{DistributedMobilityFeedRegistry, FeedSpec};
use reqwest::RequestBuilder;
use std::{fs::{self, File}, io::Write};

async fn getstatic(feed: String, url: String) {
    let client = reqwest::ClientBuilder::new().deflate(true).gzip(true).brotli(true).build().unwrap();
    let request: RequestBuilder = match feed.as_str() {
        "f-dp3-metra" => {
            client.get(&url)
                .header("username", "bb2c71e54d827a4ab47917c426bdb48c")
                .header("Authorization", "Basic YmIyYzcxZTU0ZDgyN2E0YWI0NzkxN2M0MjZiZGI0OGM6ZjhiY2Y4MDBhMjcxNThiZjkwYWVmMTZhZGFhNDRhZDI=")
        }
        "f-dqc-wmata~rail" | "f-dqc-wmata~bus" => {
            client.get(&url).header("api_key", "3be3d48087754c4998e6b33b65ec9700")
        }
        _ => {
            client.get(&url)
        }
    };
    let response = request.send().await;
    match response {
        Ok(response) => {
            let mut out = File::create(format!("gtfs/{}.zip", &feed)).expect("failed to create file");
            let bytes_result = response.bytes().await;
            if bytes_result.is_ok() {
                out.write(&bytes_result.unwrap()).unwrap();
                println!("Finished writing {}", &feed);
            }
        }
        Err(error) => {
            println!("Error with downloading {}: {}", &feed, &error);
        }
    }
}

#[tokio::main]
async fn main() {
    let gtfs_dir = arguments::parse(std::env::args()).unwrap().get::<String>("dir").unwrap_or("/home/lolpro11/Documents/Catenary/catenary-backend/gtfs_static_zips/".to_string());
    let dir = "transitland-atlas/feeds/";
    let _ = fs::create_dir("gtfs");
    let mut handles = Vec::new();
    let handle = tokio::spawn(async move {
        getstatic("f-anteaterexpress".to_string(), "https://raw.githubusercontent.com/lolpro11/gtfs-schema/main/f-anteaterexpress.zip".to_string()).await;
    });
    handles.push(handle);
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() && path.extension().unwrap_or_default() == "json" {
            let json = fs::read_to_string(&path).unwrap();
            let domain: DistributedMobilityFeedRegistry = serde_json::from_str(&json).unwrap();
            for feed in domain.feeds {
                if feed.spec == FeedSpec::Gtfs && feed.urls.static_current.as_deref().is_some() {
                    let handle = tokio::spawn(async move {
                        getstatic(feed.id, feed.urls.static_current.as_deref().unwrap_or(&"".to_string()).to_string()).await;
                    });
                    handles.push(handle);
                }
            }
        }
    };
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Error awaiting task: {}", e);
        }
    }
}