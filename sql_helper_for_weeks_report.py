get_weekly_information = """SELECT
  a.hits.page.searchKeyword as Term,
  a.sessions AS last_3_month,
  b.sessions AS last_x_week_sessions,
  ROUND(b.orders/b.sessions,2) AS last_x_week_conversion,
  ROUND(a.orders/a.sessions,2) AS last_3_month_conversion,
  a.revenue AS last_3_month_revenue,
  b.revenue AS last_x_week_revenue
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
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{date_start}'), TIMESTAMP('{date_end}')))
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
    FROM (TABLE_DATE_RANGE([{dataset_num}.ga_sessions_], TIMESTAMP('{week_start}'), TIMESTAMP('{week_end}')))
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
ORDER BY
  a.sessions DESC
limit 50;"""