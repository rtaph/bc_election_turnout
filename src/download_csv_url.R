# author: Rafael Pilliard Hellwig
# date: 2020-11-19

"Download a given CSV from the web into the data/raw directory.

Usage: src/download_csv_url.R <url> [--refresh_stale=<refresh_stale>]

Options:
--url=<url>                      URL of the CSV (without quotation marks)
--refresh_stale=<refresh_stale>  A logical indicating if outdated files should
                                 be refreshed [default: TRUE]
" -> doc

# Load packages
library(docopt)

# User-specified shell options
opt <- docopt(doc)


# Download the data
main <- function(url, refresh_stale) {
    url_str <- rlang::quo_name({{ url }})
    rs_str <- rlang::quo_name({{ refresh_stale }})
    download_csv_url(url_str, rs_str)
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
        rlang::inform(glue::glue("{file} already exists Skipping..."))
    }
    
    # Update the registry
    entry <- tibble::tibble(url = url, 
                            version_date = last_mod,
                            download_date = lubridate::ymd_hms(h$date))
    registry <- dplyr::rows_upsert(registry, entry, by = "url")
    readr::write_csv(registry, regfile)
}

main(opt$url, opt$refresh_stale)
