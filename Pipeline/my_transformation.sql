-- =====================================================================
-- Module 8: the whole expense tracker as ONE Lakeflow Declarative Pipeline
-- Target: catalog `workspace`, schema `finance_pipeline` (set in pipeline settings)
--
-- You DECLARE what each table should contain. Databricks works out the
-- order, the checkpoints, the incremental logic and the retries.
-- =====================================================================


-- ---------------------------------------------------------------------
-- BRONZE: streaming tables = Auto Loader + checkpoint, managed for you
-- (Module 7 took ~25 lines of Python per bank for this)
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_bank_a
COMMENT 'Raw savings-account statement rows, loaded incrementally'
AS SELECT
  `Date`              AS date_raw,
  Narration           AS narration,
  `Chq./Ref.No.`      AS ref_no,
  `Withdrawal Amt.`   AS withdrawal_raw,
  `Deposit Amt.`      AS deposit_raw,
  `Closing Balance`   AS balance_raw,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/finance/raw_files/landing/bank_a',
  format => 'csv', header => true, inferColumnTypes => false   -- keep everything as text in bronze
);

CREATE OR REFRESH STREAMING TABLE bronze_bank_b
COMMENT 'Raw credit-card statement rows, loaded incrementally'
AS SELECT
  `Transaction Date`  AS date_raw,
  Description         AS description_raw,
  Amount              AS amount_raw,
  Type                AS type_raw,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/finance/raw_files/landing/bank_b',
  format => 'csv', header => true, inferColumnTypes => false
);


-- ---------------------------------------------------------------------
-- SILVER: both banks in one shape (Module 2), de-duplicated (Module 4),
-- with data-quality expectations instead of hand-written filters
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW transactions_standardized
COMMENT 'Both banks converted to one common layout. Not yet validated.'
AS
WITH a AS (
  SELECT *,
    try_cast(regexp_replace(trim(withdrawal_raw), ',', '') AS DECIMAL(12,2)) AS w,
    try_cast(regexp_replace(trim(deposit_raw),    ',', '') AS DECIMAL(12,2)) AS d
  FROM bronze_bank_a
)
SELECT
  'bank_a'                                                  AS account,
  to_date(try_to_timestamp(trim(date_raw), 'dd/MM/yy'))     AS txn_date,
  trim(narration)                                           AS description,
  trim(ref_no)                                              AS bank_ref,
  CASE WHEN w > 0 THEN w ELSE d END                         AS amount,
  CASE WHEN w > 0 THEN 'DEBIT' WHEN d > 0 THEN 'CREDIT' END AS direction,
  source_file, ingested_at
FROM a
UNION ALL
SELECT
  'bank_b',
  to_date(try_to_timestamp(trim(date_raw), 'yyyy-MM-dd')),
  trim(description_raw),
  CAST(NULL AS STRING),
  abs(try_cast(regexp_replace(trim(amount_raw), ',', '') AS DECIMAL(12,2))),
  CASE WHEN upper(trim(type_raw)) IN ('DR', 'DEBIT')  THEN 'DEBIT'
       WHEN upper(trim(type_raw)) IN ('CR', 'CREDIT') THEN 'CREDIT' END,
  source_file, ingested_at
FROM bronze_bank_b;


CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  -- Expectations: rows that fail are dropped AND counted in the Data quality tab
  CONSTRAINT valid_date      EXPECT (txn_date IS NOT NULL)              ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount    EXPECT (amount IS NOT NULL AND amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_direction EXPECT (direction IS NOT NULL)             ON VIOLATION DROP ROW
)
COMMENT 'Clean, de-duplicated transactions from all accounts'
AS
WITH keyed AS (
  SELECT *,
    concat_ws('|', account, CAST(txn_date AS STRING), upper(description),
              CAST(amount AS STRING), direction, coalesce(bank_ref, '')) AS dedupe_key
  FROM transactions_standardized
),
with_ids AS (
  SELECT *,
    sha2(concat_ws('|', dedupe_key,
         CAST(row_number() OVER (PARTITION BY source_file, dedupe_key ORDER BY dedupe_key) AS STRING)),
         256) AS txn_id                                  -- occurrence-within-file trick from Module 4
  FROM keyed
)
SELECT txn_id, account, txn_date, description, bank_ref, amount, direction, source_file
FROM with_ids
QUALIFY row_number() OVER (PARTITION BY txn_id ORDER BY source_file) = 1;   -- keep one copy


CREATE OR REFRESH MATERIALIZED VIEW quarantine_transactions
COMMENT 'Rows that failed a quality rule, with the reason(s)'
AS SELECT *,
  filter(array(
    CASE WHEN txn_date IS NULL                  THEN 'valid_date'      END,
    CASE WHEN amount IS NULL OR amount <= 0     THEN 'valid_amount'    END,
    CASE WHEN direction IS NULL                 THEN 'valid_direction' END
  ), x -> x IS NOT NULL) AS failed_rules
FROM transactions_standardized
WHERE txn_date IS NULL OR amount IS NULL OR amount <= 0 OR direction IS NULL;


-- ---------------------------------------------------------------------
-- CATEGORIZED (Module 5): rules first, then the AI results you already
-- paid for, then defaults. Reads tables OUTSIDE the pipeline by full name.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW transactions_categorized
COMMENT 'Every transaction with merchant and category'
AS
WITH matches AS (
  SELECT t.*, r.merchant_name, r.category AS rule_category,
    row_number() OVER (PARTITION BY t.txn_id
                       ORDER BY r.priority ASC NULLS LAST, length(r.pattern) DESC NULLS LAST) AS rn
  FROM silver_transactions t
  LEFT JOIN workspace.finance.merchant_rules r
    ON upper(t.description) LIKE concat('%', upper(r.pattern), '%')
),
best AS (
  SELECT *,
    coalesce(merchant_name, initcap(trim(regexp_replace(
      CASE WHEN upper(description) LIKE 'UPI/%' THEN split(description, '/')[1]
           ELSE regexp_replace(regexp_replace(upper(description),
                  '^(POS|NEFT|IMPS|ACH|BILLDESK)[ /-]+', ''), '[^A-Z ]', ' ')
      END, '[ ]+', ' ')))) AS merchant
  FROM matches WHERE rn = 1
)
SELECT
  b.txn_id, b.account, b.txn_date, b.description, b.amount, b.direction, b.merchant,
  CASE WHEN b.rule_category IS NOT NULL THEN b.rule_category
       WHEN b.direction = 'CREDIT'      THEN 'Other Income'
       WHEN a.ai_category IS NOT NULL   THEN a.ai_category
       ELSE 'Uncategorized' END                                  AS category,
  CASE WHEN b.rule_category IS NOT NULL                       THEN 'rule'
       WHEN b.direction = 'DEBIT' AND a.ai_category IS NOT NULL THEN 'ai'
       ELSE 'default' END                                        AS category_source,
  CAST(date_trunc('MONTH', b.txn_date) AS DATE)                  AS month
FROM best b
LEFT JOIN workspace.finance.ai_merchant_categories a ON b.merchant = a.merchant;


-- ---------------------------------------------------------------------
-- GOLD (Module 6)
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_summary
COMMENT 'Income, spending, refunds and savings per month'
AS
WITH m AS (
  SELECT month,
    sum(CASE WHEN direction = 'CREDIT' AND category IN ('Salary', 'Other Income') THEN amount ELSE 0 END) AS income,
    sum(CASE WHEN direction = 'DEBIT'  AND category <> 'Transfer'                 THEN amount ELSE 0 END) AS gross_spend,
    sum(CASE WHEN direction = 'CREDIT' AND category = 'Refund'                    THEN amount ELSE 0 END) AS refunds
  FROM transactions_categorized
  GROUP BY month
)
SELECT *,
  gross_spend - refunds                                        AS net_spend,
  income - (gross_spend - refunds)                             AS savings,
  round((income - (gross_spend - refunds)) / nullif(income, 0), 3) AS savings_rate
FROM m;


CREATE OR REFRESH MATERIALIZED VIEW gold_category_spend
AS SELECT month, category, sum(amount) AS spent, count(*) AS txns
FROM transactions_categorized
WHERE direction = 'DEBIT' AND category <> 'Transfer'
GROUP BY month, category;


CREATE OR REFRESH MATERIALIZED VIEW gold_budget_status
COMMENT 'OK / WARNING / OVER per category per month'
AS SELECT
  s.month, s.category, b.monthly_budget, s.spent,
  b.monthly_budget - s.spent           AS remaining,
  round(s.spent / b.monthly_budget, 3) AS pct_used,
  CASE WHEN s.spent > b.monthly_budget        THEN 'OVER'
       WHEN s.spent >= 0.8 * b.monthly_budget THEN 'WARNING'
       ELSE 'OK' END                    AS status
FROM gold_category_spend s
JOIN workspace.finance.budgets b USING (category);