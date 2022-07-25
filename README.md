
# ATR Reports
## Project Objective
Find Sportsbook volume and exchange commission taken from UK/Ireland Customers on Greyhound and Horse Racing in Australia, New Zealand and South Africa.

## Project Instructions
1. **Create FPA Races Excel:** The list of fixtures is sent in an excel with a sheet containing horse racing and another containing greyhound racing. Reformat the Fixture list into one table with headers evnt_start_time, mtng_date, mtng_venue_location, evnt_number, mtng_name and add a sport_name column containing 'Greyhound Racing' for the GH Events and 'Horse Racing' for the TB Events. The important columns are evnt_start_time, mtng_name and sport_name. Save this as data/01_raw/fp_races/fp_races_(month).xlsx. (Full month name in lower case.)
3. **Run the Jupyter Notebook:** In the notebook notebooks/races_volume_commission.ipynb, set month = (month) then run all cells.
4. **Check results:** In output/(month) this will create four csv files, left_only, right_only, both and japan. Both should contain about 85% of the races in the fp_races file, if not there may be an issue with the merge. left_only contains races that were in the fp_races file but not matched to a race in the volume_commission file, right_only contains races that were in the volume_commission file but not matched to a race in the fp_races file. Check that there are no races in left_only and right_only that should have been matched with each other. If there are, the issue is likely a slight difference in the mtng_name, and you should be able to fix it by adding a regex string to 'to_replace' and its replacement to 'replacements ' in the notebook.
