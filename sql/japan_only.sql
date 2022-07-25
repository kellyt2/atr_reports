DROP TABLE
    IF EXISTS exch;
CREATE temp TABLE exch AS
(
    SELECT
        ex.ramp_event_id,
        SUM(ex.commission_apportioned_amount_gbp) AS commission,
        SUM(
            CASE
                WHEN country_of_residence_name = 'Ireland'
                THEN ex.commission_apportioned_amount_gbp
                ELSE 0
            END) AS ie_commission,
        SUM(
            CASE
                WHEN country_of_residence_name = 'United Kingdom'
                THEN ex.commission_apportioned_amount_gbp
                ELSE 0
            END) AS uk_commission
    FROM
        omni_exchange.bf_vw_exchange_bet_matched_ramp AS ex
    LEFT JOIN
        omni.dim_account da
    ON
        da.account_id = ex.account_id
    AND da.brand_nk = 'BF'
    WHERE
    DATE(ex.settled_datetime) + 7 >= '2022-04-30'
    AND DATE(ex.settled_datetime) - 7 < '2022-06-01'
    AND (
            country_name = 'Japan')
    AND sport_name IN ('Greyhound Racing', 'Horse Racing')
    AND da.account_type NOT LIKE '%Internal%'
    AND ex.country_of_residence_name IN ('Ireland',
                                         'United Kingdom')
    AND ex.account_id NOT IN (2224978,1577133,7793670,225707,5386997,5386999)
    GROUP BY
        1);
DROP TABLE
    IF EXISTS sbk;
CREATE temp TABLE sbk AS
(
    SELECT
    em.ramp_event_id,
        pp_track_name,
        sport_name,
        --em.brand,
        start_time_uki,
        event_country_name,
        SUM(em.volume_adjusted_gbp)            AS volume,
        SUM(em.revenue_gbp - ticket_stake_gbp) AS adj_revenue,
        SUM(
            CASE
                WHEN country_of_residence_name = 'Ireland'
                THEN (case when em.brand = 'BF' then em.volume_adjusted_gbp else 0 end)
                ELSE 0
            END) AS bf_ie_volume,
        SUM(
            CASE
                WHEN country_of_residence_name = 'United Kingdom'
                THEN (case when em.brand = 'BF' then em.volume_adjusted_gbp else 0 end)
                ELSE 0
            END) AS bf_uk_volume,
        SUM(
            CASE
                WHEN country_of_residence_name = 'Ireland'
                THEN (case when em.brand = 'BF' then em.revenue_gbp - ticket_stake_gbp else 0 end)
                ELSE 0
            END) AS bf_ie_adj_revenue,
        SUM(
            CASE
                WHEN country_of_residence_name = 'United Kingdom'
                THEN (case when em.brand = 'BF' then em.revenue_gbp - ticket_stake_gbp else 0 end)
                ELSE 0
            END) AS bf_uk_adj_revenue,

        SUM(
            CASE
                WHEN country_of_residence_name = 'Ireland'
                THEN (case when em.brand = 'PP' then em.volume_adjusted_gbp else 0 end)
                ELSE 0
            END) AS pp_ie_volume,
        SUM(
            CASE
                WHEN country_of_residence_name = 'United Kingdom'
                THEN (case when em.brand = 'PP'  then em.volume_adjusted_gbp else 0 end)
                ELSE 0
            END) AS pp_uk_volume,
        SUM(
            CASE
                WHEN country_of_residence_name = 'Ireland'
                THEN (case when em.brand = 'PP'  then em.revenue_gbp - ticket_stake_gbp else 0 end)
                ELSE 0
            END) AS pp_ie_adj_revenue,
        SUM(
            CASE
                WHEN country_of_residence_name = 'United Kingdom'
                THEN (case when em.brand = 'PP' then em.revenue_gbp - ticket_stake_gbp else 0 end)
                ELSE 0
            END) AS pp_uk_adj_revenue
    FROM
        omni_sportsbook.vw_bet_summary_racing_reporting em
    LEFT JOIN
        omni.dim_account da
    ON
        da.account_id = em.account_id
    AND da.brand_nk = em.brand
    WHERE
        da.account_type NOT LIKE '%Internal%'
    AND (
                event_country_name = 'Japan')

    AND (
            start_time_uki) >= '2022-04-30'
    AND (
            start_time_uki) < '2022-06-01'
    AND DATE(settled_datetime) + 7 >= '2022-04-30'
    AND DATE(settled_datetime) - 7 < '2022-06-01'
    AND em.country_of_residence_name IN ('Ireland',
                                         'United Kingdom')
    AND sport_id IN (7,4339)
    GROUP BY
        1,2,3,4,5);
SELECT
    COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT)) AS ramp_id,
    --sbk.brand,
    --sbk.country_of_residence_name,
    sbk.sport_name,
    pp_track_name                                       AS track,
    ramp.race_number                                    AS race_number,
    COALESCE(sbk.start_time_uki,ramp.actual_start_time) AS race_time,
    sbk.event_country_name                              AS country,
    bf_uk_adj_revenue,
    bf_ie_adj_revenue,
    pp_uk_adj_revenue,
    pp_ie_adj_revenue,
    adj_revenue
FROM
    sbk
FULL OUTER JOIN
    exch
ON
    sbk.ramp_event_id = exch.ramp_event_id
LEFT JOIN
    omni_betevent.ramp_vw_event AS ramp
ON
    ramp.event_id = COALESCE(CAST(sbk.ramp_event_id AS INT), CAST(exch.ramp_event_id AS INT))
WHERE
    DATE(race_time) >= '2022-04-30'
    AND DATE(race_time) < '2022-06-01'