use failure::Error;
use regex::RegexSet;
use reqwest;
use select::node::Node;
use select::document::Document;
use select::predicate::Name;
// use std::collections::HashMap;
// use crossbeam;
use std::sync::mpsc::{channel, RecvError};
use threadpool::ThreadPool;


pub struct NdbcFileCrawler<'a> {
    root_server_url: &'a str,
    root_out: &'a str,
    catalog_url: &'a str,
    pattern_list: &'a [&'a str],
    file_list: Vec<String>,
    // remote_local_hashmap: HashMap<String, String>,
}

impl<'a> NdbcFileCrawler<'a> {
    pub fn new() -> Self {
        NdbcFileCrawler {
            root_server_url: "https://dods.ndbc.noaa.gov/",
            root_out: "/mnt/glusterfs/datasets/Data/in-situ/ndbc_raw_rs/",
            catalog_url: "https://dods.ndbc.noaa.gov/thredds/catalog/data/stdmet/catalog.html",
            pattern_list: &[r".*/.h\d{4}.nc$"],
            file_list: vec![],
            // remote_local_hashmap: HashMap::new(),
        }
    }

    pub fn crawl_file_list(&'a mut self) -> Result<&'a mut NdbcFileCrawler, Error> {
        // let file_list: Vec<String> = vec![];
        let re = RegexSet::new(self.pattern_list).unwrap();

        let file_list: Vec<String> = {
            let mut file_list: Vec<String> = vec![];
            let dir_list: Vec<String> = self.get_directory_list()
                .expect("Fail to gather the remote directory list.");

            println!(">> dir_list with {} elems.", dir_list.len());

            let pool = ThreadPool::new(4);    
            let (tx, rx) = channel();
            for station_catalog_url in dir_list {
                let tx = tx.clone();
                pool.execute( || {
                    let tmp_file_list: Vec<String> = self.gather_file_list(&station_catalog_url.clone()).unwrap();
                    tx.send(tmp_file_list).expect("Could not send data!");
                });
            }
            

            for station_catalog_url in dir_list {
                // self.gather_file_list(station_catalog_url)?;
                file_list.extend(self.gather_file_list(&station_catalog_url)?);
            }
        file_list
        };

        Ok(self)
    }

    fn is_remote_dir(text: &str, href: &str) -> bool {
        if text.ends_with('/') && href.ends_with(".html") {
            true
        } else {
            false
        }
    }

    fn get_station_info(node: Node) -> (String, &str) {
        let station_id: String = node.text();
        let station_href: &str = node.attr("href")
            .expect("Cannot gather station's catalog href");
        (station_id, station_href)
    }

    pub fn get_directory_list(&self) -> Result<Vec<String>, Error> {
        let mut dir_list: Vec<String> = vec![];

        let body = reqwest::get(self.catalog_url)
            .expect(&format!(
                "Fail to request catalog url: {}",
                self.catalog_url
            ))
            .text()?;
        let document = Document::from(body.as_str());

        for (i, node) in document.find(Name("a")).enumerate() {
            let station_id: String = node.text();
            let station_href: &str = node.attr("href")
                .expect("Cannot gather station's catalog href");

            if NdbcFileCrawler::is_remote_dir(&station_id, &station_href) {
                let url_station_catalog: String = self.catalog_url.replace("catalog.html", &station_href);
                dir_list.push(url_station_catalog.clone());

                println!(
                    "{:>4} : {:>6} {:>20}  =>  {}",
                    i, station_id, station_href, url_station_catalog
                );
                // get_file_list(&url_station_catalog);
            }
        }


        Ok(dir_list)
    }


    fn gather_file_list(&self, station_catalog_url: &str) -> Result<Vec<String>, Error> {
        println!("In gather_file_list", );
        let mut file_url_list: Vec<String> = vec![];

        let body = reqwest::get(station_catalog_url)?.text()?;
        let document = Document::from(body.as_str());

        for (i, node) in document.find(Name("a")).enumerate() {
            let (station_id, station_href) = NdbcFileCrawler::get_station_info(node);
            if station_id.ends_with(".nc") && !station_id.contains("h9999") {
                let url_file_station = self.catalog_url
                    .replace("catalog.html", &station_id)
                    .replace("/catalog/", "/fileServer/");

                println!("\t{:>4} : {}", i, url_file_station);
                file_url_list.push(url_file_station.to_owned())
            }
        }
        println!("");

        Ok(file_url_list)
    }


}
