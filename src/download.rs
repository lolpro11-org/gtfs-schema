mod dmfr;
use dmfr::{DistributedMobilityFeedRegistry, FeedSpec};
use async_recursion::async_recursion;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use reqwest::Client;
use std::{collections::HashSet, fs::{self, File}, io::Write, path::PathBuf, time::Duration};

#[async_recursion]
async fn getstatic(client: &Client, feed: String, url: String) {
    loop {
        println!("Downloading {}", feed);
        let request = match feed.as_str() {
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
                break;
            }
            Err(err) => {
                println!("Error with downloading {}: {}", &feed, &err);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let threads = 100;
    let dir = "transitland-atlas/feeds/";
    fs::create_dir("gtfs").unwrap_or_default();
    let mut urls = vec![("f-anteaterexpress".to_string(), "https://raw.githubusercontent.com/lolpro11/gtfs-schema/main/f-anteaterexpress.zip".to_string())];
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() && path.extension().unwrap_or_default() == "json" {
            let json = fs::read_to_string(&path).unwrap();
            let domain: DistributedMobilityFeedRegistry = serde_json::from_str(&json).unwrap();
            for feed in domain.feeds {
                if feed.spec == FeedSpec::Gtfs {
                    if feed.urls.static_current.as_deref().is_some() {
                        urls.push((feed.id, feed.urls.static_current.as_deref().unwrap().to_string()));
                    } else if !feed.urls.static_historic.is_empty() {
                        urls.push((feed.id, feed.urls.static_historic.first().unwrap().to_string()));
                    }
                }
            }
        }
    };

    println!("{:#?}", urls);

    let mut futs = FuturesUnordered::new(); 
    for feed in 0..urls.len()-1 {
        let feed_id = urls[feed].0.clone();
        let url = urls[feed].1.clone();
        let fut = async move {
            let client = reqwest::ClientBuilder::new().cookie_store(true).deflate(true).gzip(true).brotli(true)
            .use_rustls_tls().user_agent("Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0").tcp_keepalive(Duration::new(300, 0)).build().unwrap();
            getstatic(&client, feed_id, url).await;
        };
        futs.push(fut);
        if futs.len() == threads {
            futs.next().await.unwrap();
        }
    }

    let mut downloaded = HashSet::new();
    if let Ok(entries) = fs::read_dir("gtfs") {
        for entry in entries {
            if let Ok(entry) = entry {
                let file_name = entry.file_name();
                let file_path = PathBuf::from(&file_name);
                
                if let Some(file_stem) = file_path.file_stem() {
                    if let Some(file_stem_str) = file_stem.to_str() {
                        downloaded.insert(file_stem_str.to_string());
                    }
                }
            }
        }
    } else {
        eprintln!("Error reading directory");
    }

    let mut missing =  Vec::new();
    for url in &urls {
        if !downloaded.contains(&url.0) {
            missing.push(url.clone());
        }
    }
    println!("{:#?}", missing);
    println!("Total feeds missing: {}", missing.len());

}