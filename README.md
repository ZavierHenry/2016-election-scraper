# 2016-election-scraper

Scrapes websites for total vote count in the 2016 general election by county. Sites used for scraping each state can be found in [data_scrapers/local_data/regions.csv](data_scrapers/local_data/regions.csv).

## Running the scraper

Run the scraper by using the following command:

`node d3_voter_map [<filename>]`

By default, the scraper will save the results to ./results.csv

## Collecting Alaska data

Alaska does not report its election results by county; the closest level is done by precinct. Therefore the following process was done to approximate Alaska's results by county:

1. Determine the county for each precinct. This was done by looking up the county in which each precinct polling place resided. For example, in 2016, the polling place for precinct 01-480 was located at Pioneer Park Centennial Center in Fairbanks, Alaska. Therefore, precinct 01-480 maps to county Fairbanks. A table for each precinct and county can be found in [data_scrapers/local_data/alaska_precinct_to_county.csv](data_scrapers/local_data/alaska_precinct_to_county.csv).

2. Add the totals from each precinct to their respective counties.

3. Determine the counties that comprise each district.

4. Allocate absentee ballot vote counts to each county. Because absentee ballot results are reported by district, vote counts per district are allocated based on the proportion of Election Day votes in each county.
