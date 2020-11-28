# Author: Chad Neald
# Date: 2020-11-26
#
# Usage:
# bash bc_election_turnout.sh

# Download the data
Rscript src/download_data.R \
  https://catalogue.data.gov.bc.ca/dataset/44914a35-de9a-4830-ac48-870001ef8935/resource/fb40239e-b718-4a79-b18f-7a62139d9792/download/provincial_voting_results.csv \
  https://catalogue.data.gov.bc.ca/dataset/6d9db663-8c30-43ec-922b-d541d22e634f/resource/646530d4-078c-4815-8452-c75639962bb4/download/provincial_voter_participation_by_age_group.csv \
  --path=data/raw

# Clean and process the data
Rscript src/clean_data.R \
  --pvr_input=data/raw/provincial_voting_results.csv \
  --pvp_input=data/raw/provincial_voter_participation_by_age_group.csv \
  --out_dir=data/processed

# Create the plots
Rscript src/make_figures.R \
  --input=data/processed/bc_election_by_district.rds \
  --out_dir=data/doc/images

# Run the correlation test
Rscript src/perform_statistical_test.R \
  --input=data/processed/bc_election_by_district.rds \
  --out_dir=data/processed

# Generate the report
Rscript -e "rmarkdown::render('doc/bc_election_turnout_report.Rmd')"
