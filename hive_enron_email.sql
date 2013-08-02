CREATE EXTERNAL TABLE emails (
  mid STRUCT<ts: TIMESTAMP, value: STRING>,
  dateStr STRUCT<ts: TIMESTAMP, value: STRING>,
  fromStr STRUCT<ts: TIMESTAMP, value: STRING>,
  toStr STRUCT<ts: TIMESTAMP, value: STRING>,
  subject STRUCT<ts: TIMESTAMP, value: STRING>,
  cc STRUCT<ts: TIMESTAMP, value: STRING>,
  bcc STRUCT<ts: TIMESTAMP, value: STRING>,
  body STRUCT<ts: TIMESTAMP, value: STRING>,
  sentiment STRUCT<ts: TIMESTAMP, value: FLOAT>
)
STORED BY 'org.kiji.hive.KijiTableStorageHandler'
WITH SERDEPROPERTIES (
  'kiji.columns' = 'info:mid[0],info:date[0],info:from[0],info:to[0],info:subject[0],info:cc[0],info:bcc[0],info:body[0],features:sentiment[0]'
)
TBLPROPERTIES (
  'kiji.table.uri' = 'kiji://.env/enron_email/emails'
);
