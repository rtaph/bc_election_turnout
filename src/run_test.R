# author: Rafael Pilliard Hellwig
# date: 2020-11-24

"Perform Pearson' Product Moment Correlation Test

Usage: src/run_test.R [--input=<input>] [--out_dir=<out_dir>]

Options:
--input=<input>        Path (including filename) to cleaned data (saved as
                       an RDS data frame with numeric columns `competitiveness`
                       and `turnout`) [default: data/processed/pvr_agg.rds]
--out_dir=<out_dir>    Path to directory where the results should be saved (as
                       an RDS file) [default: data/processed]
" -> doc

# load packages
library(docopt)

# load shell parameters
opt <- docopt(doc)

main <- function(data, out_dir = "data/processed") {

  # load data
  pvr_agg <- readRDS(data)

  # perform a correlation test
  test_1 <- stats::cor.test(~turnout+competitiveness, data = pvr_agg)
  tidytest_1 <- broom::tidy(test_1)

  # write the results to disk
  saveRDS(tidytest_1, here::here(out_dir, "cor_test.rds"))
}

main(opt[["--input"]], opt[["--out_dir"]])
