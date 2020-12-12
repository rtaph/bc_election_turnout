# Team: Daredevils of Data Science (Team #19)
# Authors: Kamal Moravej Jahromi, Chad Neald, Rafael Pilliard Hellwig, Yuan Xiong
# Date: 2020-12-11
# Purpose: A container with the scientific computing environment needed for the BC Election Turnout Project


# Start from a base layer with R, tidyverse, rstudio, and latex
FROM rocker/verse:4.0.3

# Configure latex and add tlmgr dependencies
RUN Rscript -e "tinytex::install_tinytex(); tinytex::tlmgr_install(c('amsmath', 'caption'))"

# Install necessary R packages from MRAN
RUN Rscript -e "install.packages(c('dataMaid', 'here', 'cowplot', 'docopt', 'GGally', 'ggpubr', 'ggthemes', 'janitor'), repos = 'https://mran.microsoft.com/snapshot/2020-12-08')"
