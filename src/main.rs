extern crate crossbeam;
extern crate failure;
extern crate num_cpus;
extern crate regex;
extern crate reqwest;
extern crate select;
extern crate threadpool;
extern crate rayon;

use failure::Error;
use select::document::Document;
use std::fs::File;
use std::path::Path;
// use std::io::prelude::*;
use select::predicate::Name;
use std::io::BufWriter;
// use reqwest::{Client, ClientBuilder};
use std::sync::mpsc::channel;
use std::sync::Arc;
use threadpool::ThreadPool;
use std::collections::HashMap;
use rayon::prelude::*;

mod crawler;
// mod ndbc_crawler;

use crawler::Crawler;

fn filter_catalog(text: &str, href: &str) -> bool {
    if text.ends_with('/') && href.ends_with(".html") {
        true
    } else {
        false
    }
}

fn get_station_info(node: select::node::Node) -> (String, &str) {
    let station_id: String = node.text();
    let station_href: &str = node.attr("href")
        .expect("Cannot gather station's catalog href");
    (station_id, station_href)
}

fn run() -> Result<(), Error> {
    let root_url: &str = "https://dods.ndbc.noaa.gov/thredds/catalog/data/";
    let fileserver_root_url: &str = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/";
    let catalog_url: &str = "https://dods.ndbc.noaa.gov/thredds/catalog/data/stdmet/catalog.html";
    let catalog_ext: &str = ".html";
    let nc_ext: &str = ".nc";
    let to_replace: &str = "catalog.html?dataset=data/";
    // let root_out: &str = "/home/meidhy/work/data/insitu/ndbc/";
    let root_out: &str = "/home/ansible/tmp/datasets/insitu/ndbc/ndbc_reqw/";

    let n_workers = 16;
    let pool = ThreadPool::new(n_workers);

    let crawl: Crawler = Crawler::with_pool_size(32);

    let fpl = crawl.list_file_by_ext(catalog_url, catalog_ext)?;
    println!(
        ">> '{}' contains '{}' '{}' files.",
        catalog_url,
        fpl.len(),
        catalog_ext,
    );

    let n_jobs = fpl.len();

    let arc_catalog_url = Arc::new(catalog_url);

    let (tx, rx) = channel();
    for fp in fpl {
        let tx = tx.clone();

        println!(">> '{}'", fp);
        let crawl: Crawler = Crawler::with_pool_size(4);

        let _catalog_url = arc_catalog_url.clone();

        pool.execute(move || {
            let station_url: &str = &_catalog_url.replace("/catalog.html", &format!("/{}", &fp));
            // println!("{}", station_url);
            tx.send({
                let nc_fpl = crawl
                    .list_file_by_ext(station_url, nc_ext)
                    .expect("Fail to gather nc file list.");

                let mut file_hashmap: HashMap<String, String> = HashMap::new();
                // let mut file_hashmap: Vec<Vec<String, String>> = vec![vec![]];
                for nc_fp in &nc_fpl {
                    let fin = nc_fp.replace(&to_replace, &fileserver_root_url);
                    let fout = nc_fp.replace(&to_replace, root_out);

                    file_hashmap.insert(fin, fout);
                    // file_hashmap.push(vec![fin, fout]);
                }

                Crawler::download_hashmap(file_hashmap).expect("Fail to download HashMap.");



                nc_fpl
                // ()
            }).expect("Fail to send nc crawler.")
        });
    }

    drop(tx);
    for t in rx.iter() {
        println!("{:>4} files downloaded.", t.len());
    }
    // rx.iter().take(n_jobs).collect::<()>();
    Ok(())
}

fn main() {
    run().expect("Reqwesting things doesn't work...");

    let url: &str = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/ykrv2/ykrv2h2017.nc";
    let fp_out: &str = "/home/meidhy/work/data/insitu/ndbc/";
    // get_single_file(url, fp_out).expect("Unable to request the file.");
}
