# author: Rafael Pilliard Hellwig
# date: 2020-11-19

"Download a given CSV from the web into the data/raw directory.

Usage: src/download_csv_url.R <urls>... [--refresh_stale=<refresh_stale>]

Options:
--urls=<urls>...                 One or more URLs pointing to onling CSV files
                                 (space separated, no quotation marks).
--refresh_stale=<refresh_stale>  A logical indicating if outdated files should
                                 be refreshed [default: TRUE]
" -> doc

# Load packages
library(docopt)
library(testthat)

# User-specified shell options
opt <- docopt(doc)


# Download the data
main <- function(urls, refresh_stale) {
    rs_str <- rlang::quo_name({{ refresh_stale }})
    urls_str <- vapply(urls, rlang::quo_name, character(1))
    for (url in urls_str) {
        download_csv_url(url, rs_str) 
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
#' @param refresh_stale A scalar logical indicating whether data should be
#'   refreshed if a more recent version exists on the provincial government's
#'   servers. Defaults to TRUE.
#'
#' @return NULL. The function is called for its data downloading side-effect.
#' @examples
#' url <- paste0("https://catalogue.data.gov.bc.ca/dataset/44914a35-de9a-48",
#'               "30-ac48-870001ef8935/resource/fb40239e-b718-4a79-b18f-7a6",
#'               "2139d9792/download/provincial_voting_results.csv")
#' download_csv_url(url)
download_csv_url <- function(url, refresh_stale = TRUE) {
    
    # Read-in registry of data source URLs
    regfile  <- here::here("data", "raw", "registry.csv")
    registry <- readr::read_csv(regfile, col_types = "cTT")
    
    # Define names and paths
    file <- basename(url)
    path <- here::here("data", "raw", file)
    
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
    
    if (!file.exists(path)) {
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

main(opt$urls, opt$refresh_stale)
