DROP TABLE
    IF EXISTS results;
CREATE temp TABLE results AS
(
    SELECT DISTINCT
        bf_ob_sh.ev_id,
        bf_ob_sh.ev_mkt_id,
        bf_ob_sh.bf_selection_id,
        result
    FROM
        bf_vw_openbet_selection_history AS bf_ob_sh
    LEFT JOIN
        bf_vw_openbet_market bf_ob_m
    ON
        bf_ob_m.ev_id = bf_ob_sh.ev_id
    AND bf_ob_m.ev_mkt_id = bf_ob_sh.ev_mkt_id
    AND bf_ob_m.market_name = 'Match Odds'
    LEFT JOIN
        bf_vw_openbet_event ev
    ON
        ev.ev_id = bf_ob_m.ev_id
    WHERE
        bf_ob_sh.result_conf_at_datetime >= SYSDATE - 1095
        and bf_ob_sh.result_conf_at_datetime < SYSDATE
        
    AND sport_id = '17');
DROP TABLE
    IF EXISTS win_void;
CREATE temp TABLE win_void AS
(
    SELECT
        A.ev_id,
        A.ev_mkt_id
    FROM
        results A,
        results B
    WHERE
        A.ev_id = B.ev_id
    AND A.ev_mkt_id = B.ev_mkt_id
    AND ((
                A.result = 'W'
            AND B.result = 'L')
        OR  (
                A.result = 'L'
            AND B.result = 'W')) );
            
DROP TABLE
    IF EXISTS expected_margin;
CREATE temp TABLE expected_margin AS
(
select source,
method,
mult_leg_id,
settled_datetime,
exp_revenue_incl_price_boost from omni_sportsbook.vw_bet_expected_margin_football_sas where settled_datetime >= sysdate - 1095 and settled_datetime < sysdate
);

DROP TABLE
    IF EXISTS ramp_history;
CREATE temp TABLE ramp_history AS
(
select event_id,
        MIN(case when extra_score_info = 'Retired' then message_timestamp else null end) as retirement_time,
        MAX(case when extra_score_info != 'Retired' then message_timestamp else null end) as feed_stop_time
        from omni_betevent.ramp_vw_event_history where scheduled_start_time >= sysdate - 1095 - 7 and scheduled_start_time < sysdate group by 1
        );

DROP TABLE
    IF EXISTS wv_bets;
CREATE temp TABLE wv_bets AS
(
    SELECT
    sbk.brand,
        sbk.mult_ref,
        sbk.country_of_residence_name,
        sbk.event_name,
        sbk.ramp_event_id,
        sbk.settled_datetime,
        sbk.placed_datetime,
        sbk.volume_gbp,
        sbk.revenue_gbp,
        sbk.total_number_legs,
        sbk.sportex_selection_name,
        sbk.bet_cash_out_yn,
        sbk.stake_factor_at_bet_group,
        case when expected_margin.method ='E' then sbk.volume_gbp else 0 end as exch_mapped_volume,
        case when expected_margin.method = 'E' then exp_revenue_incl_price_boost else 0 end as exch_mapped_revenue,
        case when expected_margin.method = 'E' then 1 else 0 end as exchange_mapped_yn ,
        retirement_time,
        feed_stop_time

    FROM
        win_void
    LEFT JOIN
        omni_sportsbook.vw_bet_summary_sas_reporting sbk
    ON
        sbk.ob_event_id = win_void.ev_id
    --AND sbk.ob_market_id = win_void.ev_mkt_id
    left join ramp_history on sbk.ramp_event_id = ramp_history.event_id
        LEFT JOIN
        expected_margin
    ON
        sbk.mult_leg_id = expected_margin.mult_leg_id
    AND sbk.settled_datetime = expected_margin.settled_datetime
    AND sbk.source = expected_margin.source
    where sbk.settled_datetime >= sysdate - 1095
    and sbk.settled_datetime < sysdate
    --and market_name != 'Match Odds'
    and sbk.in_play_yn = 'Y'
    and sbk.total_number_legs = 1 order by 3,2,1,4);
    
    
DROP TABLE
    IF EXISTS wv_time_before_retirement;
CREATE temp TABLE wv_time_before_retirement AS
(
    SELECT DISTINCT
    brand,
        mult_ref,
        country_of_residence_name,
        event_name,
        total_number_legs,
        settled_datetime,
        sportex_selection_name,
        DATEDIFF(minute, placed_datetime, retirement_time) as time_before_retirement,
        DATEDIFF(minute, placed_datetime, feed_stop_time) as time_after_feed_stop,
        volume_gbp,
        revenue_gbp,
        exch_mapped_volume,
        exch_mapped_revenue,
        exchange_mapped_yn,
        bet_cash_out_yn,
        stake_factor_at_bet_group
    FROM
        wv_bets);
        
DROP TABLE
    IF EXISTS wv_time_grouped;
CREATE temp TABLE wv_time_grouped AS(
SELECT
    country_of_residence_name,
    case when time_before_retirement > 60 then 60 else time_before_retirement end as time_to_retirement,
    count(mult_ref) as bet_count,
    sum(case when stake_factor_at_bet_group in ('0-0.01', '0.02-0.1') then 1 else 0 end) as restricted_bet_count,
     SUM(volume_gbp) AS total_volume,
     SUM(revenue_gbp) AS total_revenue ,
     SUM(exch_mapped_volume) as total_exch_mapped_volume,
     SUM(exch_mapped_revenue) as total_exch_mapped_revenue,
     SUM(exchange_mapped_yn) as total_exch_mapped
     FROM wv_time_before_retirement
     --where country_of_residence_name != 'Italy'
     group by 1,2);
SELECT country_of_residence_name,time_to_retirement, total_volume, total_revenue, total_revenue/total_volume as margin, bet_count, case when total_exch_mapped_volume = 0 then 0 else total_exch_mapped_revenue/total_exch_mapped_volume end as exch_mapped_margin, case when total_exch_mapped_volume = 0 then 0 else total_exch_mapped_volume/total_volume end as exch_mapped_perc, total_exch_mapped, total_exch_mapped_volume, total_exch_mapped_revenue from wv_time_grouped;
--select * from wv_time_before_retirement