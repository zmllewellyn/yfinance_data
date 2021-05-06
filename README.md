# yfinance_data
Scrape financial data from Yahoo Finance, clean it, then apply a small set of transformations to the data.

## Instructions for Implementation
- The only input necessary is a csv file containing the list of companies you wish to download data for (use same format as Energy.csv)
- Since each csv of the downloading/cleaning/saving is saved ensure that each section saves to a different location 
- Specifications for the date range can be made in the format '2020-04-01'

## Precautions to Take
- For hourly data it is only possible to download 730 days worth of data for a stock in one execution
- The cleaning method relies on the assumption that at least one of the input stocks will have near 100% of the data in the specified date range. To ensure that the cleaning correctly iterates over every timestamp it is recommended to include a 'dummy' large-cap stock like MSFT which 'should' have easy access to data.
- Similarly, it isn't recommended to attempt to download high frequency data (1min and below) since the current cleaning method has no easy way to generate each time series timestamp

## Possible Updates
- Proper timestamp generating function to remove the need for a 'dummy' large-cap stock
- Function allowing multiple downloads of hourly data to overcome the 730 day limit
- Further transformations to the cleaned financial time series
