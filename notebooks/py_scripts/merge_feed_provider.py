#!/usr/bin/env python
# coding: utf-8

# In[49]:


import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pyodbc


# In[50]:


# set the name of the month here
month = "june"
date = "1 " + month + ", 2022"
startdate = (datetime.strptime(date, "%d %B, %Y") - timedelta(days=1)).strftime(
    "%m/%d/%Y, %H:%M:%S"
)
enddate = (datetime.strptime(date, "%d %B, %Y") + relativedelta(months=1)).strftime(
    "%m/%d/%Y, %H:%M:%S"
)


# # Reading Feed Provider Data

# In[51]:


df_feed_provider = pd.read_excel("../data/01_raw/fp_races/fp_races_" + month + ".xlsx")
df_feed_provider["evnt_start_time"] = pd.to_datetime(
    df_feed_provider["evnt_start_time"]
)
df_feed_provider.dropna(how="all")


# # Reading Sportsbook and Exchange Data

# In[52]:


conn = pyodbc.connect("DSN=Redshift ODBC")
cursor = conn.cursor()


# In[53]:


q1 = "DROP TABLE IF EXISTS exch;"
q2 = (
    "CREATE temp TABLE exch AS ( SELECT ex.ramp_event_id, SUM(ex.commission_apportioned_amount_gbp) AS commission, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ex.commission_apportioned_amount_gbp ELSE 0 END) AS ie_commission, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ex.commission_apportioned_amount_gbp ELSE 0 END) AS uk_commission FROM omni_exchange.bf_vw_exchange_bet_matched_ramp AS ex LEFT JOIN omni.dim_account da ON da.account_id = ex.account_id AND da.brand_nk = 'BF' WHERE DATE(ex.settled_datetime) + 7 >= '"
    + startdate
    + "' AND DATE(ex.settled_datetime) - 7 < '"
    + enddate
    + "' AND ( country_name = 'Australia' OR country_name = 'New Zealand' OR country_name = 'South Africa') AND sport_name IN ('Greyhound Racing', 'Horse Racing') AND da.account_type NOT LIKE '%Internal%' AND ex.country_of_residence_name IN ('Ireland', 'United Kingdom') AND ex.account_id NOT IN (2224978,1577133,7793670,225707,5386997,5386999) GROUP BY 1);"
)
q3 = "DROP TABLE IF EXISTS sbk;"
q4 = "CREATE temp TABLE sbk AS ( SELECT em.ramp_event_id, pp_track_name, sport_name, start_time_uki, event_country_name, SUM(em.volume_adjusted_gbp) AS volume, SUM(em.revenue_gbp - ticket_stake_gbp) AS adj_revenue, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN (case when em.brand = 'BF' then em.volume_adjusted_gbp else 0 end) ELSE 0 END) AS bf_ie_volume, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN (case when em.brand = 'BF' then em.volume_adjusted_gbp else 0 end) ELSE 0 END) AS bf_uk_volume, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN (case when em.brand = 'BF' then em.revenue_gbp - ticket_stake_gbp else 0 end) ELSE 0 END) AS bf_ie_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN (case when em.brand = 'BF' then em.revenue_gbp - ticket_stake_gbp else 0 end) ELSE 0 END) AS bf_uk_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN (case when em.brand = 'PP' then em.volume_adjusted_gbp else 0 end) ELSE 0 END) AS pp_ie_volume, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN (case when em.brand = 'PP' then em.volume_adjusted_gbp else 0 end) ELSE 0 END) AS pp_uk_volume, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN (case when em.brand = 'PP' then em.revenue_gbp - ticket_stake_gbp else 0 end) ELSE 0 END) AS pp_ie_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN (case when em.brand = 'PP' then em.revenue_gbp - ticket_stake_gbp else 0 end) ELSE 0 END) AS pp_uk_adj_revenue FROM omni_sportsbook.vw_bet_summary_racing_reporting em LEFT JOIN omni.dim_account da ON da.account_id = em.account_id AND da.brand_nk = em.brand WHERE da.account_type NOT LIKE '%Internal%' AND (( event_country_name = 'Australia') OR ( event_country_name = 'New Zealand') OR ( event_country_name = 'South Africa')) AND ( start_time_uki) >= '2022-05-31' AND ( start_time_uki) < '2022-07-01' AND DATE(settled_datetime) + 7 >= '2022-05-31' AND DATE(settled_datetime) - 7 < '2022-07-01' AND em.country_of_residence_name IN ('Ireland', 'United Kingdom') AND sport_id IN (7,4339) GROUP BY 1,2,3,4,5);"
q5 = (
    "SELECT COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT)) AS ramp_id, sbk.sport_name, pp_track_name AS track, ramp.race_number AS race_number, COALESCE(sbk.start_time_uki,ramp.actual_start_time) AS race_time, sbk.event_country_name AS country, exch.commission AS commission, exch.uk_commission, exch.ie_commission, bf_uk_volume, bf_ie_volume, pp_uk_volume, pp_ie_volume, volume FROM sbk FULL OUTER JOIN exch ON sbk.ramp_event_id = exch.ramp_event_id LEFT JOIN omni_betevent.ramp_vw_event AS ramp ON ramp.event_id = COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT)) WHERE DATE(race_time) >= '"
    + startdate
    + "' AND DATE(race_time) < '"
    + enddate
    + "' "
)


# In[54]:


cursor.execute(q1)
cursor.execute(q2)
cursor.execute(q3)
cursor.execute(q4)


# In[55]:


# dataframe of data from the sportsbook and exchange
df_volume_commission = pd.read_sql(q5, conn)
df_volume_commission["race_time"] = pd.to_datetime(df_volume_commission["race_time"])


# In[56]:


j1 = "DROP TABLE IF EXISTS exch;"
j2 = (
    "CREATE temp TABLE exch AS ( SELECT ex.ramp_event_id, SUM(ex.commission_apportioned_amount_gbp) AS commission, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ex.commission_apportioned_amount_gbp ELSE 0 END) AS ie_commission, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ex.commission_apportioned_amount_gbp ELSE 0 END) AS uk_commission FROM omni_exchange.bf_vw_exchange_bet_matched_ramp AS ex LEFT JOIN omni.dim_account da ON da.account_id = ex.account_id AND da.brand_nk = 'BF' WHERE DATE(ex.settled_datetime) + 7 >= '"
    + startdate
    + "' AND DATE(ex.settled_datetime) - 7 < '"
    + enddate
    + "' AND ( country_name = 'Japan') AND sport_name IN ('Greyhound Racing', 'Horse Racing') AND da.account_type NOT LIKE '%Internal%' AND ex.country_of_residence_name IN ('Ireland', 'United Kingdom') AND ex.account_id NOT IN (2224978,1577133,7793670,225707,5386997,5386999) GROUP BY 1);"
)
j3 = "DROP TABLE IF EXISTS sbk;"
j4 = (
    "CREATE temp TABLE sbk AS ( SELECT em.ramp_event_id, pp_track_name, sport_name, start_time_uki, event_country_name, SUM(em.volume_adjusted_gbp) AS volume, SUM(em.revenue_gbp - ticket_stake_gbp) AS adj_revenue, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ( CASE WHEN em.brand = 'BF' THEN em.volume_adjusted_gbp ELSE 0 END) ELSE 0 END) AS bf_ie_volume, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ( CASE WHEN em.brand = 'BF' THEN em.volume_adjusted_gbp ELSE 0 END) ELSE 0 END) AS bf_uk_volume, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ( CASE WHEN em.brand = 'BF' THEN em.revenue_gbp - ticket_stake_gbp ELSE 0 END) ELSE 0 END) AS bf_ie_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ( CASE WHEN em.brand = 'BF' THEN em.revenue_gbp - ticket_stake_gbp ELSE 0 END) ELSE 0 END) AS bf_uk_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ( CASE WHEN em.brand = 'PP' THEN em.volume_adjusted_gbp ELSE 0 END) ELSE 0 END) AS pp_ie_volume, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ( CASE WHEN em.brand = 'PP' THEN em.volume_adjusted_gbp ELSE 0 END) ELSE 0 END) AS pp_uk_volume, SUM( CASE WHEN country_of_residence_name = 'Ireland' THEN ( CASE WHEN em.brand = 'PP' THEN em.revenue_gbp - ticket_stake_gbp ELSE 0 END) ELSE 0 END) AS pp_ie_adj_revenue, SUM( CASE WHEN country_of_residence_name = 'United Kingdom' THEN ( CASE WHEN em.brand = 'PP' THEN em.revenue_gbp - ticket_stake_gbp ELSE 0 END) ELSE 0 END) AS pp_uk_adj_revenue FROM omni_sportsbook.vw_bet_summary_racing_reporting em LEFT JOIN omni.dim_account da ON da.account_id = em.account_id AND da.brand_nk = em.brand WHERE da.account_type NOT LIKE '%Internal%' AND ( event_country_name = 'Japan') AND ( start_time_uki) >= '"
    + startdate
    + "' AND ( start_time_uki) < '"
    + enddate
    + "' AND DATE(settled_datetime) + 7 >= '"
    + startdate
    + "' AND DATE(settled_datetime) - 7 < '"
    + enddate
    + "' AND em.country_of_residence_name IN ('Ireland', 'United Kingdom') AND sport_id IN (7,4339) GROUP BY 1,2,3,4,5);"
)
j5 = (
    "SELECT COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT)) AS ramp_id, sbk.sport_name, pp_track_name AS track, ramp.race_number AS race_number, COALESCE(sbk.start_time_uki,ramp.actual_start_time) AS race_time, sbk.event_country_name AS country, bf_uk_adj_revenue, bf_ie_adj_revenue, pp_uk_adj_revenue, pp_ie_adj_revenue, adj_revenue FROM sbk FULL OUTER JOIN exch ON sbk.ramp_event_id = exch.ramp_event_id LEFT JOIN omni_betevent.ramp_vw_event AS ramp ON ramp.event_id = COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT)) WHERE DATE(race_time) >= '"
    + startdate
    + "' AND DATE(race_time) < '"
    + enddate
    + "'"
)


# In[57]:


cursor.execute(j1)
cursor.execute(j2)
cursor.execute(j3)
cursor.execute(j4)


# In[58]:


df_japan = pd.read_sql(j5, conn)


# # Cleaning the Data

# In[59]:


to_replace = [
    r" ST\'",
    r"BOWEN RIVER",
    r"MT BARKER",
    r"NEWCASTLE DG",
    r"NEWCASTLE AU",
    r"WENTWORTH PK",
    r" SCARPSIDE",
    r".\(AU\)",
    r".\(AUS\)",
    r" AUS",
    r" NZ",
    r" \(NZ\)",
    r"\(DOGS\)",
    r" DOGS",
    r"DOGS",
]
replacements = [
    " STRAIGHT",
    "Bowen",
    "MOUNT BARKER",
    "NEWCASTLE",
    "NEWCASTLE",
    "WENTWORTH PARK",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
]

replacement_dict = dict(zip(to_replace, replacements))

for key in replacement_dict.keys():
    df_volume_commission["track"] = df_volume_commission["track"].replace(
        key, replacement_dict[key], regex=True
    )


df_feed_provider_to_replace = [r"\s*Races", r"\s*Greys", r"\s*RACEWAY\s*"]
df_feed_provider_replacements = ["", "", ""]

df_feed_provider_replacement_dict = dict(
    zip(df_feed_provider_to_replace, df_feed_provider_replacements)
)

for key in df_feed_provider_replacement_dict.keys():
    df_feed_provider["Racecourse"] = df_feed_provider["mtng_name"].replace(
        key, df_feed_provider_replacement_dict[key], regex=True
    )


df_feed_provider["Racecourse"] = df_feed_provider["Racecourse"].str.upper()

df_volume_commission = df_volume_commission.rename(
    columns={
        "track": "Racecourse",
        "race_time": "evnt_start_time",
        "race_number": "evnt_number",
    }
)


df_volume_commission["Racecourse"] = df_volume_commission[
    "Racecourse"
].str.strip()
df_feed_provider["Racecourse"] = df_feed_provider["Racecourse"].str.strip()

df_volume_commission["sport_name"] = df_volume_commission[
    "sport_name"
].str.strip()
df_feed_provider["sport_name"] = df_feed_provider["sport_name"].str.strip()


# converting to local time and then getting the date, in order to merge on event number and date
df_volume_commission["Meeting Date"] = df_volume_commission[
    "evnt_start_time"
] + timedelta(hours=8)
df_volume_commission["evnt_number"] = (
    df_volume_commission["evnt_number"].astype(str).astype(int)
)


df_feed_provider["Meeting Date"] = df_feed_provider["evnt_start_time"] + timedelta(
    hours=8
)

df_volume_commission["Meeting Date"] = pd.to_datetime(
    df_volume_commission["Meeting Date"]
).dt.date
df_feed_provider["Meeting Date"] = pd.to_datetime(
    df_feed_provider["Meeting Date"]
).dt.date


# # Merging and Saving to CSVs

# In[60]:


df_merged = df_feed_provider.merge(
    df_volume_commission,
    how="right",
    on=["Racecourse", "Meeting Date", "evnt_number", "sport_name"],
    indicator=True,
)


# In[61]:


df_right_only = df_merged[
    (df_merged["_merge"] == "right_only") & (df_merged["country"] != "South Africa")
]
df_right_only.to_csv("../reports/figures/" + month + "/right_only.csv", index=False)


# In[62]:


df_left_merged = df_feed_provider.merge(
    df_volume_commission,
    how="left",
    on=["Racecourse", "Meeting Date", "evnt_number"],
    indicator=True,
)
df_left_only = df_left_merged[df_left_merged["_merge"] == "left_only"]
df_left_only.to_csv("../reports/figures/" + month + "/left_only.csv", index=False)


# In[63]:


df_merged_both = df_merged[
    ((df_merged["_merge"] == "both") | (df_merged["country"] == "South Africa"))
]
df_merged_both.to_csv("../reports/figures/" + month + "/both.csv", index=False)


# In[64]:


df_japan.to_csv("../reports/figures/" + month + "/japan.csv", index=False)

