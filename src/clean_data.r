"This script read the CSV data from a given path and process it.
At the end, it write the processed csv data to the given path.

Usage: src/load_process.R --pvr_input=<pvr> --pvp_input=<pvp> --out_dir=<path>

Options:
--pvr_input=<pvr>                 The file path of provintial_voting_results.csv

--pvp_input=<pvp>                 The file path of provincial_voter_participation_by_age_group.csv

--out_dir=<path>                  Path to where the processed data should be saved.
                                  This can be a relative or an absolute path.
" -> doc

library(tidyverse)
library(here)
library(docopt)
library(httr)
library(testthat)

opt <- docopt(doc)

main <- function(pvr, pvp, out_dir){

  

  # Check if the pvr path ends in .csv
  if(!str_detect(pvr, "\\.csv$")){
      stop(paste("Error. The following path does not end in .csv: ", pvr))
  }

  # Check if the pvp path ends in .csv
  if(!str_detect(pvp, "\\.csv$")){
    stop(paste("Error. The following path does not end in .csv: ", pvp))
  }
    
  # reading data set from a given path.
  pvr <- read_csv(pvr)
  pvp <- read_csv(pvp)

  # Processing the data
  # BC_election_processed_joined <- process_step(pvr, pvp)

  

  pvp <- janitor::clean_names(pvp)
  pvr <- janitor::clean_names(pvr)

  # Aggregate participation by event and electoral district
  pvp_agg_process <- pvp %>% 
    group_by(event_name, ed_name) %>%
    summarise(across(participation:registered_voters, sum),
              .groups = "drop") %>%
    mutate(turnout = participation / registered_voters)

  # Aggregate election results by event and electoral district. 
  pvr_agg_process <- pvr %>%
    filter(vote_category == "Valid") %>%
    group_by(event_name, ed_name, affiliation) %>%
    summarise(votes = sum(votes_considered),
              .groups = "drop_last") %>%
    arrange(event_name, ed_name, desc(votes)) %>%
    mutate(vote_share = votes / sum(votes),
           rank = row_number(),
           vote_trail = votes - first(votes) ,
           share_trail = vote_share - first(vote_share),
           vote_diff = nth(vote_trail, 2),
           competitiveness = nth(share_trail, 2),
           winning_party = nth(affiliation, 1)) %>%
    nest(candidates = c(affiliation, votes, vote_share, vote_trail,
                        share_trail, rank)) %>%
    ungroup %>%
    left_join(pvp_agg_process, by = c("event_name", "ed_name"))


  # saving a processed csv data in a given path
  dir.create(here(out_dir), recursive = TRUE)
  file = here::here(out_dir, "bc_election_by_district.rds")
  saveRDS(pvr_agg_process, file = file)

} 

main(opt$pvr_input, opt$pvp_input, opt$out_dir)
#main(opt[["--input"]], opt[["--out_dir"]])