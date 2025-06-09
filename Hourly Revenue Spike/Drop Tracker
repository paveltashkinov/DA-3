/*
Hourly Revenue Spike/Drop Tracker

Objective:
Develop a tool to detect abnormal hourly revenue spikes or drops by comparing current hour performance to the same hour on the previous day.

Requirements:
Compare each hour (e.g., 12 PM on 6/8/25 vs. 12 PM on 6/7/25).
Allow dynamic baseline resets (e.g., to account for advertiser changes).
Highlight significant deviations (e.g., ±X% threshold).
*/

with precalc as
(
    select site_id
         , toDate(event_date) td
         , toHour(event_date) th
         , sum(sum_response_price)      AS revenue_response
         , sum(sum_response_price_pub)  AS revenue_pub_response
         , sum(sum_win_price)       AS revenue_wins
         , sum(sum_win_price_pub)       AS revenue_pub_wins    
      from header_bidder.statistic_group 
     where event_date >= toDate(now() - INTERVAL 1 DAY)
       and site_id = 11373 ---for example
  group by site_id
         , toDate(event_date)
         , toHour(event_date)
),

unpv as
(
    select site_id, td, th, 'revenue_response' metric, case when td = toDate(now()) then 1 else -1 end * revenue_response value
      from precalc
 union all
     select site_id, td, th, 'revenue_pub_response' metric, case when td = toDate(now()) then 1 else -1 end *revenue_pub_response value
      from precalc
 union all
     select site_id, td, th, 'revenue_wins' metric, case when td = toDate(now()) then 1 else -1 end *revenue_wins value
      from precalc
 union all
     select site_id, td, th, 'revenue_pub_wins' metric, case when td = toDate(now()) then 1 else -1 end *revenue_pub_wins value
      from precalc
)
    select site_id, td, th, metric, value
         , sum(value)over(partition by site_id, th, metric) abs_difference
         , case when abs(sum(value)over(partition by site_id, th, metric)) > 5 then 1 else 0 end is_highlighted_deviation
      from unpv
  order by metric, th, td desc 
