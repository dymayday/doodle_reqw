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
    let root_url: &str = "https://dods.ndbc.noaa.gov/thredds/catalog/data/";
    let fileserver_root_url: &str = "https://dods.ndbc.noaa.gov/thredds/fileServer/";
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
            }).expect("Fail to send nc crawler.")
        });
    }

    drop(tx);
    for t in rx.iter() {
        println!("{:>4} files downloaded.", t.len());
    }

    Ok(())
}

fn main() {
    run().expect("Reqwesting things doesn't work...");

    let url: &str = "https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/ykrv2/ykrv2h2017.nc";
    let fp_out: &str = "/home/meidhy/work/data/insitu/ndbc/";
    // get_single_file(url, fp_out).expect("Unable to request the file.");
}
