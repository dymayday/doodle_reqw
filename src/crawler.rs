use failure::Error;
use reqwest;
use select::document::Document;
// use select::node::Node;
use select::predicate::Name;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
// use std::sync::mpsc::{channel, RecvError};
// use threadpool::ThreadPool;
use num_cpus;
use rayon::prelude::*;

pub struct Crawler {
    cpu_count: usize,
}

impl Crawler {
    pub fn new() -> Self {
        Crawler {
            cpu_count: num_cpus::get(),
        }
    }

    pub fn with_pool_size(cpu_count: usize) -> Self {
        Crawler {
            cpu_count: cpu_count,
        }
    }

    pub fn create_parent_directory(file_name: &str) -> Result<(), Error> {
        //Recursively create all of the parent components of a file if they are missing.
        let dir_name: &str = ::std::path::Path::new(file_name).parent().unwrap().to_str().unwrap();
        if !::std::path::Path::new(dir_name).exists() {
            // fs::create_dir_all(dir_name).unwrap();
            ::std::fs::create_dir_all(dir_name)?;
        }
        Ok(())
    }

    fn get_document(url: &str) -> Result<Document, Error> {
        // Ok(Document::from(
        //     reqwest::get(url)
        //         .expect(&format!("Fail to request url: {}", url))
        //         .text()?
        //         .as_str(),
        // ))

        Ok(Document::from_read(
            reqwest::get(url).expect(&format!("Fail to request url: {}", url)),
        )?)
    }

    /// List a directory
    pub fn list_file_by_ext<'a>(self, url: &'a str, ext: &'a str) -> Result<Vec<String>, Error> {
        // let mut dir_list: Vec<String> = Vec::new();

        let document: Document = Crawler::get_document(url)?;

        let dir_list = document
            .find(Name("a"))
            .filter_map(|n| n.attr("href"))
            .map(|n| n.to_string())
            .filter(|n| n.ends_with(ext))
            .collect::<Vec<String>>();

        Ok(dir_list)
    }

    pub fn download_hashmap(file_hashmap: HashMap<String,String>) -> Result<(), Error> {

        // for (url, file_out) in file_hashmap.iter() {
        file_hashmap.into_par_iter().map(|(url, file_out)| {
            // println!("url: '{}', fout: '{}'", url, file_out);
            Crawler::create_parent_directory(&file_out);
            let mut response = reqwest::get(&url).expect("Fail to reqwest nc file.");
            ::std::io::copy(
                &mut response,
                &mut BufWriter::new(File::create(&file_out)
                    .expect(&format!("Fail to create local file {}", &file_out)))
                );

        });
        
        Ok(())
    }
}
