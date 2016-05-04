pdt_reco = """SELECT
  *
FROM (
  SELECT
    'organic' AS medium,
    visitors AS num_of_visitors,
    ROUND(visitors_share*100,2) AS share_of_visitors,
    sessions AS num_of_sessions,
    ROUND(sessions_share*100,2) AS share_of_sessions,
    session_duration_min,
    revenue AS loose_revenue,
    ROUND(revenue_share*100,2) AS share_of_loose_revenue,
    ROUND(conversion*100,2) AS loose_conversion_rate,
    ROUND(basket_size,2) AS loose_bakset_size,
    revenue_per_session AS loose_revenue_per_session from(
    SELECT
      GROUP,
      sessions,
      RATIO_TO_REPORT(sessions) OVER (ORDER BY sessions DESC) sessions_share,
      visitors,
      RATIO_TO_REPORT(visitors) OVER (ORDER BY visitors DESC) visitors_share,
      orders,
      RATIO_TO_REPORT(orders) OVER (ORDER BY orders DESC) orders_share,
      revenue,
      RATIO_TO_REPORT(revenue) OVER (ORDER BY revenue DESC) revenue_share,
      ROUND(orders/visitors,4) AS conversion,
      ROUND(session_duration/60, 2) AS session_duration_min,
      basket_size,
      ROUND(revenue/sessions,2) AS revenue_per_session,
    FROM (
      SELECT
        'total sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        trafficSource.medium == 'organic'),
      #filter the traffic
      (
      SELECT
        'recomendation sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        hits.eventInfo.eventCategory CONTAINS 'ProductRecommendation'
        AND trafficSource.medium == 'organic' #filter the traffic
        )) #conversion condition
  WHERE
    GROUP == 'recomendation sessions'),
  (
  SELECT
    'direct' AS medium,
    visitors AS num_of_visitors,
    ROUND(visitors_share*100,2) AS share_of_visitors,
    sessions AS num_of_sessions,
    ROUND(sessions_share*100,2) AS share_of_sessions,
    session_duration_min,
    revenue AS loose_revenue,
    ROUND(revenue_share*100,2) AS share_of_loose_revenue,
    ROUND(conversion*100,2) AS loose_conversion_rate,
    ROUND(basket_size,2) AS loose_bakset_size,
    revenue_per_session AS loose_revenue_per_session from(
    SELECT
      GROUP,
      sessions,
      RATIO_TO_REPORT(sessions) OVER (ORDER BY sessions DESC) sessions_share,
      visitors,
      RATIO_TO_REPORT(visitors) OVER (ORDER BY visitors DESC) visitors_share,
      orders,
      RATIO_TO_REPORT(orders) OVER (ORDER BY orders DESC) orders_share,
      revenue,
      RATIO_TO_REPORT(revenue) OVER (ORDER BY revenue DESC) revenue_share,
      ROUND(orders/visitors,4) AS conversion,
      ROUND(session_duration/60, 2) AS session_duration_min,
      basket_size,
      ROUND(revenue/sessions,2) AS revenue_per_session,
    FROM (
      SELECT
        'total sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        trafficSource.medium == '(none)'),
      #filter the traffic
      (
      SELECT
        'recomendation sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        hits.eventInfo.eventCategory CONTAINS 'ProductRecommendation'
        AND trafficSource.medium == '(none)' #filter the traffic
        )) #conversion condition
  WHERE
    GROUP == 'recomendation sessions'),
  (
  SELECT
    'except affiliates' AS medium,
    visitors AS num_of_visitors,
    ROUND(visitors_share*100,2) AS share_of_visitors,
    sessions AS num_of_sessions,
    ROUND(sessions_share*100,2) AS share_of_sessions,
    session_duration_min,
    revenue AS loose_revenue,
    ROUND(revenue_share*100,2) AS share_of_loose_revenue,
    ROUND(conversion*100,2) AS loose_conversion_rate,
    ROUND(basket_size,2) AS loose_bakset_size,
    revenue_per_session AS loose_revenue_per_session from(
    SELECT
      GROUP,
      sessions,
      RATIO_TO_REPORT(sessions) OVER (ORDER BY sessions DESC) sessions_share,
      visitors,
      RATIO_TO_REPORT(visitors) OVER (ORDER BY visitors DESC) visitors_share,
      orders,
      RATIO_TO_REPORT(orders) OVER (ORDER BY orders DESC) orders_share,
      revenue,
      RATIO_TO_REPORT(revenue) OVER (ORDER BY revenue DESC) revenue_share,
      ROUND(orders/visitors,4) AS conversion,
      ROUND(session_duration/60, 2) AS session_duration_min,
      basket_size,
      ROUND(revenue/sessions,2) AS revenue_per_session,
    FROM (
      SELECT
        'total sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        NOT (trafficSource.medium CONTAINS 'affi')),
      #filter the traffic
      (
      SELECT
        'recomendation sessions' AS GROUP,
        COUNT(visitId) AS sessions,
        SUM(totals.visits) AS visitors,
        SUM(totals.transactions) AS orders,
        ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
        AVG(totals.transactions) AS basket_size,
        AVG(totals.timeOnSite) AS session_duration
      FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
      WHERE
        hits.eventInfo.eventCategory CONTAINS 'ProductRecommendation'
        AND NOT (trafficSource.medium CONTAINS 'affi') #filter the traffic
        )) #conversion condition
  WHERE
    GROUP == 'recomendation sessions');"""

search_report = """SELECT
  GROUP,
  sessions AS search_sessions,
  ROUND(conversion,2) AS conversion_rate,
  revenue AS source_gross_revenue,
  orders,
  revenue_per_session from(
  SELECT
    GROUP,
    sessions,
    RATIO_TO_REPORT(sessions) OVER (ORDER BY sessions DESC) sessions_share,
    visitors,
    RATIO_TO_REPORT(visitors) OVER (ORDER BY visitors DESC) visitors_share,
    orders,
    RATIO_TO_REPORT(orders) OVER (ORDER BY orders DESC) orders_share,
    revenue,
    RATIO_TO_REPORT(revenue) OVER (ORDER BY revenue DESC) revenue_share,
    ROUND(orders/sessions,4) AS conversion,
    ROUND(session_duration/60, 2) AS session_duration_min,
    basket_size,
    ROUND(revenue/sessions,2) AS revenue_per_session,
  FROM (
    SELECT
      'total sessions' AS GROUP,
      SUM(totals.visits) AS sessions,
      COUNT(DISTINCT fullvisitorid) AS visitors,
      SUM(totals.transactions) AS orders,
      ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
      AVG(totals.transactions) AS basket_size,
      AVG(totals.timeOnSite) AS session_duration
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}'))) ),
    (
    SELECT
      'search sessions' AS GROUP,
      SUM(totals.visits) AS sessions,
      COUNT(DISTINCT fullvisitorid) AS visitors,
      SUM(totals.transactions) AS orders,
      ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue,
      AVG(totals.transactions) AS basket_size,
      AVG(totals.timeOnSite) AS session_duration
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL));"""

term_report = """SELECT
  a.hits.page.searchKeyword as Term,
  c.sessions AS yesterday_sessions,
  b.sessions AS last_week_sessions,
  a.sessions AS last_month_sessions,
  ROUND(100*c.orders/c.sessions,2) AS yesterday_conversion,
  ROUND(100*b.orders/b.sessions,2) AS last_week_conversion,
  ROUND(100*a.orders/a.sessions,2) AS last_month_conversion,
  c.revenue AS yesterday_day_revenue,
  b.revenue AS last_week_revenue,
  a.revenue AS last_month_revenue
FROM (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{month_start}'), TIMESTAMP('{month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) a
LEFT JOIN (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{week_start}'), TIMESTAMP('{month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) b
ON
  a.hits.page.searchKeyword = b.hits.page.searchKeyword
LEFT JOIN (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{day_start}'), TIMESTAMP('{month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) c
ON
  b.hits.page.searchKeyword = c.hits.page.searchKeyword
ORDER BY
  a.sessions DESC
limit 50;"""


term_report_2 = """SELECT
  a.hits.page.searchKeyword as Term,
  d.sessions AS last_week_sessions,
  c.sessions AS last_month_sessions,
  b.sessions AS last_3month_sessions,
  a.sessions AS last_6month_sessions,
  ROUND(d.orders/d.sessions,2) AS last_week_conversion,
  ROUND(c.orders/c.sessions,2) AS last_month_conversion,
  ROUND(b.orders/b.sessions,2) AS last_3month_conversion,
  ROUND(a.orders/a.sessions,2) AS last_6month_conversion,
  d.revenue AS last_week_revenue,
  c.revenue AS last_month_revenue,
  b.revenue AS last_3month_revenue,
  a.revenue AS last_6month_revenue
FROM (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{six_month_start}'), TIMESTAMP('{six_month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) a
LEFT JOIN (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{three_month_start}'), TIMESTAMP('{six_month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) b
ON
  a.hits.page.searchKeyword = b.hits.page.searchKeyword
LEFT JOIN (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{month_start}'), TIMESTAMP('{six_month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) c
ON
  b.hits.page.searchKeyword = c.hits.page.searchKeyword
LEFT JOIN (
  SELECT
    hits.page.searchKeyword,
    SUM(totals.visits) AS sessions,
    SUM(totals.transactions) AS orders,
    ROUND(SUM(totals.totalTransactionRevenue)/1000000) revenue from(
    SELECT
      hits.page.searchKeyword,
      totals.visits,
      totals.totalTransactionRevenue,
      totals.transactions
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{week_start}'), TIMESTAMP('{six_month_end}')))
    WHERE
      hits.page.searchKeyword IS NOT NULL)
  GROUP BY
    hits.page.searchKeyword
  ORDER BY
    sessions DESC
  LIMIT
    1000) d
ON
  c.hits.page.searchKeyword = d.hits.page.searchKeyword 
ORDER BY
  a.sessions DESC
limit 50;"""

site_report = """select 
IF(device_category == 'all','all',device.deviceCategory) as device_category,
IF(device.deviceCategory == 'mobile',device.operatingSystem,device_os) as device_os,
round(100*product_pageviews/visits,2) as cr_pp,
round(100*add_to_cart/product_pageviews,3) as cr_add_to_cart,
round(100*success/add_to_cart,2) as cr_success,
round(100*orders/visits,2) as cr_total,
orders,
round(revenue/orders) as avg_order_value,
basket_size,
visits,
round(100*bounces/visits,2) as bounce_rate,
round(revenue/visits,2) as revenue_per_session
from
(SELECT
  device.deviceCategory,
  'all' as device_os,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'productView',1,0)) AS product_pageviews,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'addToCart',1,0)) AS add_to_cart,
  SUM(IF(totals.transactions > 0,1,0)) AS success,
  SUM(totals.visits) AS visits,
  SUM(totals.bounces) AS bounces,
  SUM(totals.transactions) AS orders,
  ROUND(SUM(totals.totalTransactionRevenue)/1000000,2) revenue,
  ROUND(AVG(totals.transactions),2) AS basket_size,
  ROUND(AVG(totals.timeOnSite),2) AS session_duration
FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
WHERE
  device.deviceCategory = 'desktop'
  OR device.deviceCategory = 'tablet'
GROUP BY
  device.deviceCategory),
  (SELECT
  device.deviceCategory,
  device.operatingSystem,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'productView',1,0)) AS product_pageviews,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'addToCart',1,0)) AS add_to_cart,
  SUM(IF(totals.transactions > 0,1,0)) AS success,
  SUM(totals.visits) AS visits,
  SUM(totals.bounces) AS bounces,
  SUM(totals.transactions) AS orders,
  ROUND(SUM(totals.totalTransactionRevenue)/1000000,2) revenue,
  ROUND(AVG(totals.transactions),2) AS basket_size,
  ROUND(AVG(totals.timeOnSite),2) AS session_duration
FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
WHERE
  device.deviceCategory = 'mobile'
    AND (device.operatingSystem = 'Android'
      OR device.operatingSystem = 'iOS')
GROUP BY
  device.deviceCategory,
  device.operatingSystem),
    (SELECT
  'all' as device_category,
  'all' as device_os,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'productView',1,0)) AS product_pageviews,
  SUM(IF(hits.eventInfo.eventCategory CONTAINS 'addToCart',1,0)) AS add_to_cart,
  SUM(IF(totals.transactions > 0,1,0)) AS success,
  SUM(totals.visits) AS visits,
  SUM(totals.bounces) AS bounces,
  SUM(totals.transactions) AS orders,
  ROUND(SUM(totals.totalTransactionRevenue)/1000000,2) revenue,
  ROUND(AVG(totals.transactions),2) AS basket_size,
  ROUND(AVG(totals.timeOnSite),2) AS session_duration
FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
WHERE
  device.deviceCategory = 'mobile'
  or device.deviceCategory = 'tablet'
  or device.deviceCategory = 'desktop');"""

temp_profile = """SELECT
  *
FROM (
  SELECT
    a.fullVisitorId,
    a.visitStartTime,
    a.fv
  FROM (
    SELECT
      fullVisitorId,
      visitStartTime,
      FIRST_VALUE(hits.eventInfo.eventAction) OVER (PARTITION BY fullVisitorId ORDER BY hits.time DESC) AS fv
    FROM (TABLE_DATE_RANGE([58090804.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
    WHERE
      hits.eventInfo.eventCategory == 'productView') a
  JOIN EACH(
    SELECT
      fullVisitorId,
      MAX(visitStartTime) AS max_visitStartTime
    FROM (TABLE_DATE_RANGE([58090804.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
    GROUP BY
      fullVisitorId) b
  ON
    a.fullVisitorId = b.fullVisitorId
    AND a.visitStartTime = b.max_visitStartTime)
GROUP BY
  a.fullVisitorId,
  a.visitStartTime,
  a.fv;"""

historic_profile = """SELECT
  fullVisitorId,
  visitStartTime,
  hits.eventInfo.eventAction
FROM (TABLE_DATE_RANGE([58090804.ga_sessions_], TIMESTAMP('{date_begin}'), TIMESTAMP('{date_end}')))
WHERE
  hits.eventInfo.eventCategory == 'productView';"""