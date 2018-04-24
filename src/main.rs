extern crate crossbeam;
extern crate failure;
extern crate regex;
extern crate reqwest;
extern crate select;
extern crate threadpool;

use select::document::Document;
use select::predicate::{Attr, Class, Name, Predicate};
// use reqwest;
use failure::Error;
use std::fs::File;
use std::path::Path;
// use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
// use reqwest::{Client, ClientBuilder};

mod ndbc_crawler;

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

fn get_file_list(catalog_url: &str) -> Result<Vec<String>, Error> {
    let mut file_url_list: Vec<String> = vec![];

    let body = reqwest::get(catalog_url)?.text()?;
    let document = Document::from(body.as_str());

    for (i, node) in document.find(Name("a")).enumerate() {
        let (station_id, station_href) = get_station_info(node);
        if station_id.ends_with(".nc") && !station_id.contains("h9999") {
            let url_file_station = catalog_url
                .replace("catalog.html", &station_id)
                .replace("/catalog/", "/fileServer/");
            // println!(
            //     "\t{:>4} : {:>6} {:>20}  =>  {}",
            //     i, station_id, station_href, url_file_station
            // );
            println!("\t{:>4} : {}", i, url_file_station);
        }
    }
    println!("");

    Ok(file_url_list)
}

fn run_old() -> Result<(), Error> {
    let root_file_url =
        "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/46237/46237h2007.nc";
    let _file_url = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/46237/46237h2007.nc";
    let url_file_template = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/{}/{}";

    let root_url = "https://dods.ndbc.noaa.gov/thredds/catalog/data/stdmet/";
    let catalog_url = "https://dods.ndbc.noaa.gov/thredds/catalog/data/stdmet/catalog.html";
    // let url: &str = "";
    let body = reqwest::get(catalog_url)?.text()?;

    let document = Document::from(body.as_str());

    let mut href_vec: Vec<String> = vec![];

    for (i, node) in document.find(Name("a")).enumerate() {
        let station_id: String = node.text();
        let station_href: &str = node.attr("href")
            .expect("Cannot gather station's catalog href");

        if filter_catalog(&station_id, &station_href) {
            let url_station_catalog: String = format!("{}{}", root_url, station_href);
            href_vec.push(url_station_catalog.clone());

            println!(
                "{:>4} : {:>6} {:>20}  =>  {}",
                i, station_id, station_href, url_station_catalog
            );
            get_file_list(&url_station_catalog);
        }
    }
    println!("");

    Ok(())
}

fn get_single_file(url: &str, root_out: &str) -> Result<(), Error> {
    let fp_out: &str = Path::new(url).file_name().unwrap().to_str().unwrap();
    let fp_out = Path::new(&root_out)
        .join(fp_out)
        .to_string_lossy()
        .to_owned()
        .to_string();

    let mut response = reqwest::get(url)?;
    println!("{:#?}", response);

    println!("{:#?}", response.url().path_segments().unwrap());

    let mut dest = {
        // extract target filename from URL
        let fname = response
            .url()
            .path_segments()
            .and_then(|segments| segments.last())
            .and_then(|name| if name.is_empty() { None } else { Some(name) })
            .unwrap_or("tmp.bin");

        println!("file to download: '{}'", fname);
        // let fname = tmpfile::name;
        // println!("will be located under: '{:?}'", fname);
        // create file with given name inside the temp dir
        // File::create(fname)?
        let fp_out = Path::new(&root_out)
            .join(fname)
            .to_string_lossy()
            .to_owned()
            .to_string();
        println!("will be located under: '{:?}'", fp_out);
        &mut BufWriter::new(File::create(&fp_out)?)
    };
    // data is copied into the target file
    ::std::io::copy(&mut response, &mut dest)?;

    Ok(())
}

fn run() -> Result<(), Error> {
    let mut ndbc_crawler: ndbc_crawler::NdbcFileCrawler = ndbc_crawler::NdbcFileCrawler::new();
    // let dir_list = ndbc_crawler.get_directory_list()?;

    // ndbc_crawler.crawl_file_list()?;
    // ndbc_crawler.crawl_file_list_crossbeam()?;
    ndbc_crawler.crawl_file_list_threadpool()?;

    Ok(())
}

fn main() {
    println!("Hello, world!");
    run().expect("Reqwesting things doesn't work...");

    let url: &str = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/ykrv2/ykrv2h2017.nc";
    let fp_out: &str = "/home/meidhy/work/data/insitu/ndbc/";
    // get_single_file(url, fp_out).expect("Unable to request the file.");
}
