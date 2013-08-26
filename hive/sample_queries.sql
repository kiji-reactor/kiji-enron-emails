;; Create the employee table
CREATE EXTERNAL TABLE employee (
  mid ARRAY<STRUCT<ts: TIMESTAMP, value: STRING>>,
  fromStr STRUCT<ts: TIMESTAMP, value: STRING>,
  toStr ARRAY<STRUCT<ts: TIMESTAMP, value: STRING>>,
  subject ARRAY<STRUCT<ts: TIMESTAMP, value: STRING>>,
  body ARRAY<STRUCT<ts: TIMESTAMP, value: STRING>>
) STORED BY 'org.kiji.hive.KijiTableStorageHandler'
WITH SERDEPROPERTIES (
  'kiji.columns' = 'sent_messages:mid,sent_messages:from[0],sent_messages:to,sent_messages:subject,sent_messages:body'
) TBLPROPERTIES (
  'kiji.table.uri' = 'kiji://.env/enron_email/employee'
);

;; Who sends the most emails
SELECT 
  fromStr.value AS fromStr,
  size(mid) AS count 
FROM employee 
ORDER BY count DESC 
LIMIT 10;

;; Email pairs
SELECT
  fromStr,
  trim(splitToStr) AS toStr,
  count(1) AS count
FROM (
  SELECT
    fromStr.value AS fromStr,
    toLine.value AS toLine
  FROM employee 
  LATERAL VIEW 
    explode(toStr) tos AS toLine
) sq LATERAL VIEW 
  explode(split(toLine,',')) tol AS splitToStr
GROUP BY fromStr, trim(splitToLine)
ORDER BY count DESC
LIMIT 10;

;; Sentiment over time
SELECT
  ((year(datelong.ts)-1999)*52+weekofyear(datelong.ts))
    AS weeknum,
  avg(sentiment.value) AS avgsentiment,
  stddev(sentiment.value) AS stddevsentiment,
  count(1) AS nummessages
FROM emails
WHERE regexp_replace(fromStr.value,".*@","")=="enron.com"  
GROUP BY ((year(datelong.ts)-1999)*52+weekofyear(datelong.ts));

;; Negative words
SELECT
  lword AS word,
  sum(sentiment) AS totalsentiment
FROM (
  SELECT 
    lower(word) AS lword,
    sentiment.value AS sentiment
  FROM usersentemails 
  LATERAL VIEW explode(sentences(body.value)[0]) wds AS word 
  WHERE regexp_replace(fromStr.value,".*@","")=="enron.com"
) subquery
GROUP BY lword
ORDER BY totalsentiment ASC;
