#' Download Project Data From the Web
#'
#' Downloads necessary CSV files from the web for data analysis, refreshing
#' outdated CSVs as needed.
#'
#' See Elections BC's Open Data License for details on terms:
#' https://elections.bc.ca/docs/EBC-Open-Data-Licence.pdf
#'
#' @param refresh_stale A scalar logical indicating whether data should be
#'   refreshed if a more recent version exists on the provincial government's
#'   servers. Defaults to TRUE.
#'
#' @return NULL. The function is called for its data downloading side-effect.
#' @examples
#' download_project_data()
download_project_data <- function(refresh_stale = TRUE) {
    
    # Read-in table of data source URLs
    regfile  <- here::here("data", "registry.csv")
    registry <- readr::read_csv(regfile, col_types = "ccT")
    
    # Download CSVs as needed
    for (i in seq_along(registry$url)) {
        url  <- registry$url[[i]]
        file <- basename(url)
        path <- here::here("data", file)
        
        if (!file.exists(path)) {
            # Download if missing
            rlang::inform(glue::glue("{file} not present. Downloading..."))
            download.file(url = url, destfile = path)
        } else if (refresh_stale) {
            # Download only if a more recent version exists
            h <- httr::HEAD(url)
            last_mod <- lubridate::dmy_hms(h$headers$`last-modified`)
            if (last_mod > registry$version_date[[i]]) {
                rlang::inform(glue::glue("{file} is stale. Downloading..."))
                download.file(url = url, destfile = path)
                registry$version_date[[i]] <- last_mod
                readr::write_csv(registry, regfile)
            } else {
                rlang::inform(glue::glue("{file} already up-to-date. Skipping..."))
            } 
        }
    }
}
