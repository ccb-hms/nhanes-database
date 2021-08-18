# docker build --progress plain --tag nhanes-workbench .

FROM hmsccb/analytic-workbench:version-1.3.0

# need old version of rvest in order for the hack that parses URLs to work
RUN Rscript -e "remove.packages('rvest')"
RUN Rscript -e "remotes::install_cran('rvest', repos='https://cran.microsoft.com/snapshot/2021-01-29')"

