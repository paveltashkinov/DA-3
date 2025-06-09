/*
Hourly Revenue Spike/Drop Tracker

Objective:
Develop a tool to detect abnormal hourly revenue spikes or drops by comparing current hour performance to the same hour on the previous day.

Requirements:
Compare each hour (e.g., 12 PM on 6/8/25 vs. 12 PM on 6/7/25).
Allow dynamic baseline resets (e.g., to account for advertiser changes).
Highlight significant deviations (e.g., ±X% threshold).
*/

with td as
(
    select site_id
         , toHour(event_date) th
         , sum(sum_response_price)      AS revenue_response
         , sum(sum_response_price_pub)  AS revenue_pub_response
         , sum(sum_win_price)       AS revenue_wins
         , sum(sum_win_price_pub)       AS revenue_pub_wins    
      from header_bidder.statistic_group 
     where event_date <= toDate(now() - INTERVAL 1 DAY)
       and event_date > toDate(now() - INTERVAL 2 DAY)
       and site_id = 11373 ---for example
  group by site_id
         , toHour(event_date)
), 

yd as
(
    select site_id
         , toHour(event_date) th
         , sum(sum_response_price)      AS revenue_response
         , sum(sum_response_price_pub)  AS revenue_pub_response
         , sum(sum_win_price)       AS revenue_wins
         , sum(sum_win_price_pub)       AS revenue_pub_wins    
      from header_bidder.statistic_group 
     where event_date <= toDate(now() - INTERVAL 2 DAY)
       and event_date > toDate(now() - INTERVAL 3 DAY)
       and site_id = 11373 ---for example 
  group by site_id
         , toHour(event_date)
)
    select coalesce(td.site_id, yd.site_id) site_id
         , coalesce(td.th, yd.th)  hr
         
         , td.revenue_response td_rev_response
         , yd.revenue_response yd_rev_response
         , 100 * abs(td.revenue_response - yd.revenue_response) / yd.revenue_response delta_rev_response
         
         , td.revenue_pub_response td_rev_pub_response
         , yd.revenue_pub_response yd_rev_pub_response
         , 100 * abs(td.revenue_pub_response - yd.revenue_pub_response) / yd.revenue_pub_response delta_rev_pub_response
         
         , td.revenue_wins td_rev_wins
         , yd.revenue_wins yd_rev_wins
         , 100 * abs(td.revenue_wins - yd.revenue_wins) / yd.revenue_wins delta_rev_wins
         
         , td.revenue_pub_wins td_rev_pub_wins
         , yd.revenue_pub_wins yd_rev_pub_wins
         , 100 * abs(td.revenue_pub_wins - yd.revenue_pub_wins) / yd.revenue_pub_wins delta_rev_pub_wins
      from td
 full join yd on td.site_id = yd.site_id 
             and td.th = yd.th
  order by hr
    
