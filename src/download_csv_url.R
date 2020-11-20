# author: Rafael Pilliard Hellwig
# date: 2020-11-19

"Download a given CSV from the web into the repository

Usage: src/download_csv_url.R <urls>... [--relpath=<relpath>] [--refresh_stale=<refresh_stale>]

Options:
--urls=<urls>...                 One or more URLs pointing to online CSV files
                                 (space separated, no quotation marks). 
                                 Required argument.
--relpath=<relpath>              A path indicating the directory where the 
                                 data will be saved, relative to the project
                                 root [default: data/raw]
--refresh_stale=<refresh_stale>  A logical indicating if existing outdated 
                                 files should be refreshed if a more recent 
                                 version is available online [default: TRUE]
" -> doc

# Load packages
library(docopt)
library(testthat)
library(fs)

# User-specified shell options
opt <- docopt(doc)


# Download the data
main <- function(urls, relpath, refresh_stale) {
    .refresh_stale <- rlang::quo_name({{ refresh_stale }})
    .relpath <- rlang::quo_name({{ relpath }})
    .urls <- vapply(urls, rlang::quo_name, character(1))
    for (url in .urls) {
        download_csv_url(url, .relpath, .refresh_stale) 
    }
}


#' Download Single Data CSV From the Web
#'
#' Downloads a single CSV file from the web for data analysis, refreshing
#' outdated versions as needed.
#'
#' See Elections BC's Open Data License for details on terms:
#' https://elections.bc.ca/docs/EBC-Open-Data-Licence.pdf
#'
#' @param url A scalar character with the URL. Must end in ".csv".
#' @param relpath A scalar character indicating the path of directory where
#'   data should be saved (relative to the project root). Defaults to 
#'   "data/raw"
#' @param refresh_stale A scalar logical indicating whether data should be
#'   refreshed if a more recent version exists on the provincial government's
#'   servers. Defaults to TRUE.
#'
#' @return NULL. The function is called for its data downloading side-effect.
#' @examples
#' url <- "https://people.sc.fsu.edu/~jburkardt/data/csv/trees.csv"
#' download_csv_url(url, refresh_stale = TRUE)
download_csv_url <- function(url, relpath = "data/raw", refresh_stale = TRUE) {
    
    # Read-in registry of data source URLs (hidden file)
    regfile  <- here::here("data",".dataregistry")
    if (!file_exists(regfile)) {
        readr::write_file("url,version_date,download_date", regfile)
    }
    registry <- readr::read_csv(regfile, col_types = "cTT")
    
    # Define names and paths
    file <- path_file(url)
    path <- here::here(fs::path(relpath), file)
    
    # Assert that the target directory exists
    if (!dir_exists(fs::path(here::here(), relpath))) {
        rlang::abort(glue::glue("{path} does not exist. Please create it."))
    }
    
    # Check that the URL is valid and ends in '.csv'
    rex_url <- "(\\b(https?|ftp|file)://)?[-a-z0-9+&@#/%?=~_|!:,.;]+csv$"
    if (!grepl(rex_url, url, ignore.case = TRUE)) {
        rlang::abort("Invalid URL. Must be a URL ending in '.csv'")
    }
    
    # Check the timestamp of when the CSV was last modified on the server
    h <- httr::HEAD(url)
    if (h$status_code != 200) {
        rlang::abort("Problem with server connection. Aborting.")
    }
    last_mod <- lubridate::dmy_hms(h$headers$`last-modified`)
    
    if (!file_exists(path)) {
        # Download if missing
        rlang::inform(glue::glue("{file} not present. Downloading..."))
        download.file(url = url, destfile = path)
    } else if (refresh_stale) {
        # Download only if a more recent version exists
        i <- which(registry$url == url)
        if (last_mod > registry$version_date[[i]]) {
            rlang::inform(glue::glue("{file} is stale. Downloading..."))
            download.file(url = url, destfile = path)
        } else {
            rlang::inform(glue::glue("{file} already up-to-date. Skipping..."))
        } 
    } else {
        rlang::inform(glue::glue("{file} already exists. Skipping..."))
    }
    
    # Update the registry
    entry <- tibble::tibble(url = url, 
                            version_date = last_mod,
                            download_date = lubridate::ymd_hms(h$date))
    registry <- dplyr::rows_upsert(registry, entry, by = "url")
    readr::write_csv(registry, regfile)
}

# Unit tests
test_that("Malformed and non-CSV URLs should error", {
    msg <- "Must be a URL ending in '.csv'"
    expect_error(download_csv_url("abc"), msg)
    expect_error(download_csv_url("google.com"), msg)
})

# This test is greyed-out to avoid blasting servers with repeated calls
#test_that("Malformed and non-CSV URLs should error", {
#    skip_if_offline()
#    toy_csv <- "https://people.sc.fsu.edu/~jburkardt/data/csv/trees.csv"
#    expect_message(download_csv_url(toy_csv), "not present. Downloading")
#    expect_message(download_csv_url(toy_csv, refresh_stale = FALSE),
#                   "already exists")
#    file.remove(here::here("data", "raw", "trees.csv"))
#})

main(opt$urls, opt$relpath, opt$refresh_stale)
