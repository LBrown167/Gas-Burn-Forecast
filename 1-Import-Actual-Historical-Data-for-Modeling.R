# Header ---------------------------------------------
# This script is really just for the initial pull of actuals historical data contained in databases
# Required for the gas burn forecast model
# And some of the QC required to make sure that the data being pulled is the correct data
# Data should date back to 2016-01-01 

# 1. Actual IFM - PDR
# 2. Actual SCE Load - EFP
# 3. Actual CAISO Load - EFP
# 4. Actual Outage (SP-15 and NP-15) - CDS
# 5. Actual Gas Price - Entegrate
# 6. Actual Weather - EFP
# 7. Actual Burns - Entegrate
# 8. Actual Renewable Generation - MV-90
# 9. Actual IFM Results - PDR
# 10. Actual Energy Imports - Avery's Tool
# The IMHR variable is calculated at the bottom of the script the data are merged

# Libraries and paths ---------------------------------------------

library(ROracle) # Connect to Oracle databases
library(data.table)
library(lubridate)
library(openxlsx)

# Path to this folder
path <- dummy_path

# Initiate Database Connection and Queries  ---------------------------------------------

# Call the R script containing the Oracle connections and SQL queries

source(file.path(path, "2-Code/2-Market-Data-Preparation", "0-SQL-Queries-Historical-Actuals.R"))

# Load the data ---------------------------------------------

# Load mapping of the gas unit resource IDs to the gas burn project naming
map <- fread(file.path(path, "1-Modified-Data", "1-Gas-Mapping-Resource-ID-to-GB-Notation.csv"))
trans_map <- data.table(read.xlsx(file.path(path,"1-Modified-Data", "Transmission-Interties.xlsx")))

# Load the data from the databases
act_ifm <- data.table(fetch(res_act_ifm)) # IFM actuals
act_sce_load <- data.table(fetch(res_act_sce_load)) # SCE load actuals
act_caiso_load <- data.table(fetch(res_act_caiso_load)) # CAISO load actuals
act_outage <- data.table(fetch(res_act_outage)) # Thermal outage actuals
act_gas_price <- data.table(fetch(res_act_gas_price)) # Gas price actuals 
act_weather <- data.table(fetch(res_act_weather)) # Weather data actuals
act_burns <- data.table(fetch(res_act_burns)) # Gas burns actuals
act_rengen <- data.table(fetch(res_act_rengen)) # Wind and solar actuals
act_ifm_awards <- data.table(fetch(res_act_ifm_awards)) # IFM award actuals
# act_imports <- data.table(fetch(res_act_imports)) # Import actuals
act_imports <- data.table(fread(file.path(path, "0-Raw-Data/OASIS Intertie Flow for Lily.csv")))


#act_rengen_mv90 <- data.table(fetch(res_act_rengen_mv90)) # Renewable generation actuals - mv90
#act_rengen_cdas <- data.table(fetch(res_act_rengen_cdas)) # Renewable generation actuals - cdas


# 1. Transform IFM Actuals  ---------------------------------------------
# Query pulls IFM data for SP-15 from OASIS (original source)
# Traders use average ON and OFF Peak prices 
# Sensible method for model using daily data 

# Change column names to lower, change certain column names, and change date format (remove time element)
colnames(act_ifm) <- tolower(colnames(act_ifm))
setnames(act_ifm, old = c("trade_date", "he"), new = c("date", "HE"))
act_ifm[, date := as.Date(date, "%d-%b-%y")]

# Obtain ON and OFF peak IFM prices for daily models
# ON PEAK = Mon through Sat HE 7-22 (no Sunday)
# OFF PEAK = Mon through  Sat HE 23 - HE 6 and ALL SUNDAY
# There are some missing
act_ifm_daily <- act_ifm[, .(on_peak_ifm = mean(lmp[HE >= 7 & HE <= 22]),
                             off_peak_ifm = mean(lmp[HE < 7 | HE > 22])),
                             by = .(date)]

# Obtain the hourly LMP prices for hourly model
# Not needed for now
# act_ifm_hourly <- act_ifm[, .(date, HE, LMP)]

# Remove no longer needed ifm data
rm(res_act_ifm, act_ifm)

# 2. Transform SCE Load Actuals  ---------------------------------------------
# The data is HB - date is fine, but hour needs + 1
# The actual load values are mostly within the MWH QUANTITY (Actuals) variable
# However, there are a few reasons that load quantites in the MWH QUANTITY column are not the real load
# 1) There is an error in the data acquisition pipeline
# If this occurs, the real actual load is entered into the ADJUSTED MWH QUANTITY column
# 2) There is a DR event
# In cases of a DR event, the DR MW are added back to the measured load to get the actual load value
# The SCE INTERRUPT MWH QUANTITY column contains the load on the DR event day with the MW added back in.
# The logic is if SCE INTERRUPT MWH QUANTITY exists that is the actual load value
# Then if ADJUSTED MWH QUANTITY exists that is the actual load value
# And if neither of those exist then the MWH QUANTITY is the actual load

# The date-time format (which is the only form that the date and time come in)
# If DST == TRUE, then date-time + 1 else date-time + 2
# The time is also HB which is why there is at least a +1 in all cases
# On the day that DST begins, there is a duplicate value
# The correct value is the value for which dst = FALSE
act_sce_load <- act_sce_load[, DT := as.Date(DT, "%d-%b-%y")]
act_sce_load[, dst := dst(DATE_TIME)]

# Apply the logic to get the correct HE
act_sce_load <- act_sce_load[, HE := ifelse(dst == TRUE, hour(DATE_TIME) + 1,
                                            hour(DATE_TIME) + 2)][order(DT, HE)]
act_sce_load <- act_sce_load[, HE := as.integer(gsub(25, 1, HE))][order(DT, HE)]

# Keep the relevant columns
act_sce_load <- act_sce_load[, .(DATE = DT, HE, MWH_QUANTITY, ADJUSTED_MWH_QUANTITY, SCE_INTERRUPT_MWH_QUANTITY)]

# Apply logic to get the actual load values outlined above
# So that I can validate date-times against Eric's SY values
act_sce_load[, adj_actuals := ifelse(!is.na(SCE_INTERRUPT_MWH_QUANTITY), SCE_INTERRUPT_MWH_QUANTITY, ADJUSTED_MWH_QUANTITY)]
act_sce_load[, sce_load := ifelse(!is.na(adj_actuals), adj_actuals, MWH_QUANTITY)]

# Get the daily sce load and peak load and LOWER CASEEEEEEEEE
act_sce_load_daily <- act_sce_load[, .(sce_load = sum(sce_load),
                                       sce_peak_load = max(sce_load)), by = .(DATE)]
colnames(act_sce_load_daily) <- tolower(colnames(act_sce_load_daily))

# Remove no longer needed sce load data
rm(res_act_sce_load, act_sce_load)

# 3. Transform CAISO Load Actuals  ---------------------------------------------
# Adapted from Frank's SQL Query OASIS SYSTEM DMND - Forecast vs Actual Transformed.SQL
# Data is from CAISO OASIS

# Column names to lowercase, create date column, change col names
colnames(act_caiso_load) <- tolower(colnames(act_caiso_load))
setnames(act_caiso_load, old = c("he", "trade_date"), new = c("HE","date"))
act_caiso_load[, date := as.Date(date, "%d-%b-%y")]

# Retain a copy of the daily data
# Not needed for now
# act_caiso_load_hourly <- copy(act_caiso_load)

# Get the daily caiso load and peak load values 
act_caiso_load_daily <- act_caiso_load[, .(caiso_load = sum(caiso_load),
                                           caiso_peak_load = max(caiso_load)), by = .(date)]

# Remove no longer needed CAISO load data
rm(res_act_caiso_load, act_caiso_load)

# 4. Transform Actual Outages  ---------------------------------------------
# Query pulls outages from OASIS (original source)
# Traders average of the summation of the hourly thermal outage values for these two nodes: 
# NP-15, SP-15, and ZP26which are the only ones in the OASIS data
# Data has a HE25 for all days which is usually NA

# Column names to lowercase, create date column, change col names
colnames(act_outage) <- tolower(colnames(act_outage))
act_outage[, date := as.Date(trade_date, "%d-%b-%y")]
act_outage <- act_outage[, .(date, he, node_id, outage)]

# Reshape the dataset - Themal outages for in CAISO territory
# act_outage <- melt(act_outage, id = 1:3, variable.name = "HE", value.name = "thermal_outage")
# act_outage <- act_outage[, HE := as.integer(gsub("he", "", HE))][order(node_id, date, HE)]

# Summarize the total number of NA values present in the data
# That are not occurring in the HE25
setnames(act_outage, old = "he", new = "HE")
#act_outage[HE != 25, sum(is.na(thermal_outage))]
#na <- act_outage[HE != 25 & is.na(thermal_outage)]

# Flag the thermal outages that are missing
# act_outage <- act_outage[, na_flag := as.integer(HE != 25 & HE != 1 & is.na(thermal_outage))]
# act_outage <- act_outage[, outage := ifelse(na_flag == 1, shift(thermal_outage), thermal_outage)]
          
# Sum the thermal outages for NP-15 and SP-15 and get one value per date and hour
# Repace the HE in the HE column with nothing - get rid of it so that only the number remains
#act_outage_hourly <- act_outage[, .(outage = sum(outage)), by = .(date, HE)]

# Get the daily average thermal outage value for the daily dataset 
# na.rm = TRUE to deal with the NA values than inhabit the HE25 slot
act_outage_daily <- act_outage[, .(outage = mean(outage, na.rm = TRUE)), by = .(date, node_id)]
act_outage_daily <- act_outage_daily[, .(outage = sum(outage, na.rm = TRUE)), by = .(date)]

# Remove no longer needed outage data
rm(res_act_outage, act_outage)

# 5. Transform Actual Gas Price Data ---------------------------------------------
  
# Column names to lowercase, create date column, keep columns that matter 
colnames(act_gas_price) <- tolower(colnames(act_gas_price))
act_gas_price[, date := as.Date(trade_date, "%d-%b-%y")]
act_gas_price <- act_gas_price[, .(date, loc, price)]

# Tranform the data to wide format
act_gas_price <- dcast(act_gas_price, date ~ loc, value.var = "price")

# Rename the actual gas price table 
# Columns to lowercase and rename variables
act_gas_price_daily  <- act_gas_price
colnames(act_gas_price_daily) <- tolower(colnames(act_gas_price_daily))
setnames(act_gas_price_daily, old = c("pgecg", "socal", "socal city-gate"),
                              new = c("pge_gp", "sc_gp", "sgcg_gp"))

# Remove no longer needed gas price data
rm(res_act_gas_price, act_gas_price)

# 6. Transform Actual Weather Data ---------------------------------------------
# There are a number of duplicate values in this data
# Some of the duplicates are exact and are duplicate dates with different weather vars

# Column names to lower, change column names, reformat date variable
colnames(act_weather) <- tolower(colnames(act_weather))
act_weather[, dt := as.Date(dt, "%d-%b-%y")]
setnames(act_weather, old = c("he", "dt"), new = c("HE", "date"))

# Handle the duplicates:
# Get make a max_date column that has latest date that the weather variables were updated
# Then filter for the entries that were modified last
# There are remaining duplicates that are exact or have same modified date/time, but different weather values
# For these, choose randomly
act_weather[, max_date := max(mod_date), by = .(station, date, HE)]
act_weather <- act_weather[mod_date == max_date]
act_weather <- unique(act_weather, by = c("date", "station", "HE"))

# Check to see whether all the duplicates have been removed - test table should be empty
act_weather[, count := .N, by = .(station, date)]
act_weather <- act_weather[, dup_flag := as.integer(count > 25)]
test <- act_weather[dup_flag == 1]

# Remove the unnecessary columns 
act_weather[, `:=` (mod_date = NULL,
                    max_date = NULL,
                    count = NULL,
                    dup_flag = NULL)]

# Tranform the dataset so that the weather station and weather variable type can be combined
# Combine the station and weather variable type columns and remove the the distinct columns
act_weather <- melt(act_weather, id = 1:3, variable.name = "weather_var", value.name = "weather")
act_weather <- act_weather[, station := paste0(station,"_",weather_var)][, weather_var := NULL]

# Tranform again to wide format so that the station_weather variable each has column
# Not needed for now
#act_weather_hourly <- dcast(act_weather, date + HE ~ station, value.var = "weather")

# temp, wbt, dewpoint, wind_chill, wind_dir, cloud_cover, sunshine, humidity, heat_index
# Remmove the weather variable keeping only the summary variables
act_weather_daily <- act_weather[, .(max = max(weather),
                                     min = min(weather),
                                     mean = mean(weather)),
                                     by = .(station, date)]

# Create the HDD and CDH variables
# hdd = 65 - mean(temp) if mean(temp) < 65 and zero otherwise
# cdh = mean(temp) - 75 if mean(temp) > 75 and zero otherwise
act_weather_daily[station %like% "temp", `:=` (hdd = pmax(65 - mean,0),
                                               cdd = pmax(mean - 75, 0)),
                                               by = .(station, date)]

# Tranform the dataset so that the weather station/weather variable type can be combined with the summary stat
act_weather_daily <- melt(act_weather_daily, id = 1:2, variable.name = "stat_var", value.name = "stat")
act_weather_daily <- act_weather_daily[, station := paste0(station,"_",stat_var)][, stat_var := NULL]

# Tranform again to wide format so that the station_weather_stat variable each has column
act_weather_daily <- dcast(act_weather_daily, date ~ station)

# This is a disgusting and horrible way to remove all the NA variables
get_na <- lapply(act_weather_daily, function(x){sum(is.na(x))})
get_na <- do.call("data.table", get_na)
get_na <- as.data.table(t(get_na), keep.rownames = "col")
get_na <- get_na[V1 == 0]

# Get the list of variables to keep and then keep only those variables
keep <- intersect(names(act_weather_daily), get_na$col)
act_weather_daily <- act_weather_daily[, ..keep]

# Remove no longer needed weather data
rm(res_act_weather, act_weather)

# 7. Transform Actual Burns ---------------------------------------------

# Column names to lower, change column names, reformat date variable
colnames(act_burns) <- tolower(colnames(act_burns))
setnames(act_burns, old = "fd", new = "date")
act_burns <- act_burns[, date := as.Date(date, "%d-%b-%y")]

# Fix the format of the units - lowercase and standard format
act_burns[, unit := tolower(unit)]
act_burns[, unit := gsub("burn", "burn_", unit)]
act_burns[, unit := gsub("-", "", unit)]
act_burns[, unit := gsub(" ", "", unit)]

# There are a couple of pipelines in here - I don't think we care
# I just think we want the total - "CALIFORNIA BORDER"    "EPNG/SOCAL-EHRENBERG" "SoCal Citygate" 
# Get the total
act_burns <- act_burns[, daily_burn := sum(fuel_burn_mmbtu, na.rm = TRUE), by = .(date, unit)][, pool := NULL]
act_burns <- unique(act_burns, by = c("date", "unit"))

# Transform the data into wide format
act_burns <- dcast(act_burns, date ~ unit, value.var = "daily_burn")

# Replace all the NA values with zeros
act_burns[is.na(act_burns)] <- 0

# Rename actual burns
act_burns_daily <- act_burns

# Obtain a total burns column by summing rows
act_burns_daily[, total_burns := rowSums(act_burns_daily[, 2:ncol(act_burns)])]

# Remove no longer needed actual burns
rm(res_act_burns, act_burns)

# 8. Transform Actual Renewable Generation (Wind + Solar) ---------------------------------------------
# There are a bunch of hours missing and solar generation is negative in hours without sunlight
# Most missing hours are the 24th hour, but also plenty of other hours are missing too
# Investigation below: there should be as many hours as there are dates
# The negative night hours for solar generation indicates that
# The solar and renewable actuals include the channel that encompasses usage as well 

# Column names to lower, change column names, reformat date variable
colnames(act_rengen) <- tolower(colnames(act_rengen))
act_rengen[, trade_date := as.Date(trade_date, "%d-%b-%y")]
setnames(act_rengen, old = c("he", "trade_date"), new = c("HE", "date"))

# uniqueN(act_rengen$date)
# act_rengen[HE == 23, .N, by = "gen"]

# Transform the data to get one column for each gen type
act_rengen <- dcast(act_rengen, date + HE ~ gen, value.var = "mw")

# Change the name of the wind and solar actual variables
setnames(act_rengen, old = c("SLR_RENEW_FCST_ACT_MW", "WND_RENEW_FCST_ACT_MW"),
                           c("sgen", "wgen")) 

# Get the count of hours by date
# Then look at incomplete days - 27% of days are missing at least one hour
# And 12% missing 2+ hours
act_rengen[, n_hours := .N, by = "date"]
test <- act_rengen[n_hours != 24]
uniqueN(test$date) # 312 dates with a missing hour somewhere
test[n_hours == 23, uniqueN(date)] # 56% of these dates missing something are missing 1 hour

# Retain a copy of the daily data - no need for hourly values at the moment
#act_rengen_hourly <- copy(act_rengen)

# Get the number of days between the start and end date
max_rengen <- max(act_rengen$date)
min_rengen <- min(act_rengen$date)

# Get the sequence of dates from start to end of dataset and copy to that there are
# 24 rows for each date then get HE 1 - 24 for each date
date_table <- data.table(date = seq(from = min_rengen, to = max_rengen, by = 1))
date_table <- date_table[rep(seq(.N), 24)][order(date)]
date_table[, HE := seq(from = 1, to = .N), by = "date"]

# Merge the rengen table to the date table to be able to truly
# Visualize the gaps in dates and hours
act_rengen <- merge(act_rengen, date_table, by = c("date", "HE"), all = TRUE)

# This takes care of most of the missing values by filling down with no NA values
act_rengen <- act_rengen[, sgen := zoo::na.locf(sgen, na.rm = FALSE), by = "date"]
act_rengen <- act_rengen[, wgen := zoo::na.locf(wgen, na.rm = FALSE), by = "date"]

# Check the status of the missing data - eh good enough
act_rengen[, miss := sum(is.na(sgen)), by = "date"]
test <- act_rengen[miss != 0 & miss != 24]
test[, uniqueN(date), by = "miss"]
act_rengen[, miss := NULL]

# Replace the negative values with 0 - should only apply to solar
act_rengen <- replace(act_rengen, act_rengen < 0, 0)

# Get the daily total for wind and solar generation
act_rengen_daily <- act_rengen[ , .(sgen = sum(sgen, na.rm = TRUE),
                                    wgen = sum(wgen, na.rm = TRUE)),
                                    by = .(date)]

# Remove no longer needed actual renewable generation
act_rengen_daily <- replace(act_rengen_daily, act_rengen_daily == 0, NA)


# 9. Transform Actual IFM awards ---------------------------------------------

# Column names to lower, change column names, reformat date variable
colnames(act_ifm_awards) <- tolower(colnames(act_ifm_awards))
setnames(act_ifm_awards, old = "flow_date", new = "date")
act_ifm_awards <- act_ifm_awards[, date := as.Date(date, "%d-%b-%y")]

# Obtain relevant columns
act_ifm_awards <- act_ifm_awards[, .(resource_id, date, da_market_energy_award)]
# <- unique(act_ifm_awards, by = "resource_id")

# Merge the ifm awards with the mapping to the gas burn resource names
# Remove the ones are are not mapped
act_ifm_awards  <- merge(act_ifm_awards, map, by = "resource_id")
act_ifm_awards <- act_ifm_awards[gas_mapping != "none"]

# Obtain the summary 
act_ifm_awards <- act_ifm_awards[, .(total_ifm = sum(da_market_energy_award, na.rm = TRUE)), by = .(gas_mapping, date)]
act_ifm_awards <- act_ifm_awards[, ifm_flag :=  as.integer(total_ifm != 0)]

# Transform the data into wide format
act_ifm_awards <- dcast(act_ifm_awards, date ~ gas_mapping, value.var = "ifm_flag")

# Create a new date lag
act_ifm_awards <- act_ifm_awards[, lead_date := shift(date, 1, type = "lead")]

# Extract column names and add lag24 to the names
# Replace column names 
names <- colnames(act_ifm_awards)
names <- paste0("lag24_", names)
colnames(act_ifm_awards) <- names

# Rename what will become the date column and remove the old datew which is flow date 
# Want the date to be the date after the flow date
setnames(act_ifm_awards, old = "lag24_lead_date", new = "date")
act_ifm_awards <- act_ifm_awards[,-1]

# 10. Transform Actual Imports ---------------------------------------------
# For each transmission intertie there are two values for each date
# An import and export direction - where export is negative and import is positive
# Avery gave me the list of tranmission interties relevant to SP-15
# They are in the dataset "trans_map" flagged with a "1"
# I removed a few from the list he gave me due to incomplete date ranges
# iid-sdge, iid-sce, sylmar_sim

# Column names to lower, change column names, reformat date variable
colnames(act_imports) <- tolower(colnames(act_imports))
act_imports <- act_imports[, date := as.Date(date, "%Y-%m-%d")]

# Pull only the import direction 
act_imports <- act_imports[ti_direction == "I"]

# Reformat the ti_names (remove ITC and to lowercase)
act_imports[, ti_id := gsub("ITC","import", ti_id)]
act_imports[, ti_id := tolower(ti_id)]

# Make a list of the intertie ids that Avery gave and filter for only the imports direction
ti_list <- trans_map[flag == 1]
act_imports <- act_imports[ti_id %in% ti_list$ti_id]

# Check the data for missing values - NAs and hours
# Weird hours surrounding daylight savings time
sum(is.na(act_imports$ene_import_mw))
#act_imports[, range(date), by = .(ti_id)]
act_imports[, count := .N, by = .(ti_id, date)]
unique(act_imports$count)

# Get a value for the average daily imports (MW)
act_imports_daily <- act_imports[, .(avg_import = mean(ene_import_mw, na.rm = TRUE)), by = .(date, ti_id)]

# Wide format
act_imports_daily <- dcast(act_imports_daily, date ~ ti_id, value.var = "avg_import") 

# Merge the data together, check dates, and filter dates  ---------------------------------------------
# 1. Actual IFM - PDR
# 2. Actual SCE Load - EFP
# 3. Actual CAISO Load - PDR
# 4. Actual Outage (SP-15 and NP-15) - CDS
# 5. Actual Gas Price - Entegrate
# 6. Actual Weather - EFP
# 7. Actual Burns - Entegrate
# 8. Actual Renewable Generation - MV-90
# 9. Actual IFM Results - PDR
# 10. Actual Energy Imports - Avery's Tool

data_daily <- merge(act_ifm_daily, act_sce_load_daily, by = "date", all = TRUE) 
data_daily <- merge(data_daily, act_caiso_load_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_outage_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_gas_price_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_burns_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_rengen_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_weather_daily, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_ifm_awards, by = "date", all = TRUE)
data_daily <- merge(data_daily, act_imports_daily, by = "date", all = TRUE)

# Check the min dates
min(act_ifm_daily$date)
min(act_sce_load_daily$date)
min(act_caiso_load_daily$date)
min(act_outage_daily$date)
min(act_gas_price_daily$date)
min(act_burns_daily$date)
min(act_rengen_daily$date)
min(act_weather_daily$date)
min(act_ifm_awards$date)

# Some of the queries have future dates/spots for future dates
# This is the easiest way to just filter to be not past the present
#data_daily <- data_daily[date <= Sys.Date()]
data_daily <- data_daily[date <= "2019-04-24"] # Last date of Avery's avaliable data this time

# Calculate On Peak and Off Peak IMHR
# On Peak =  on_peak_ifm/sgcg_gp
# Off Peak = off_peak_ifm/sgcg_gp
data_daily <- data_daily[, `:=` (imhr_on_peak = on_peak_ifm/sgcg_gp,
                                 imhr_off_peak = off_peak_ifm/sgcg_gp)]


# Export data  ---------------------------------------------

save(data_daily, file = file.path(path, "1-Modified-Data", "1-Actuals-Historical.RData"))