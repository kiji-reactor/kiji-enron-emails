CREATE TABLE emails WITH DESCRIPTION 'Enron emails by user'
ROW KEY FORMAT (from STRING, timestamp LONG)
WITH LOCALITY GROUP default
  WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Information about an email message' (
    mid "string" WITH DESCRIPTION 'Message-ID',
    date "long" WITH DESCRIPTION 'Date',
    from "string" WITH DESCRIPTION 'From',
    to "string" WITH DESCRIPTION 'To',
    subject "string" WITH DESCRIPTION 'Subject',
    cc "string" WITH DESCRIPTION 'cc',
    bcc "string" WITH DESCRIPTION 'bcc',
    body "string" WITH DESCRIPTION 'Message body'
  ),
  FAMILY features WITH DESCRIPTION 'Derived features' (
    sentiment "float" WITH DESCRIPTION 'sentiment score'
  )
);


CREATE TABLE employee WITH DESCRIPTION 'Enron emails by sender'
ROW KEY FORMAT (employee STRING)
WITH LOCALITY GROUP default
  WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY sent_messages WITH DESCRIPTION 'Information about a sent email message' (
    mid "string" WITH DESCRIPTION 'Message-ID',
    date "long" WITH DESCRIPTION 'Date',
    from "string" WITH DESCRIPTION 'From',
    to "string" WITH DESCRIPTION 'To',
    subject "string" WITH DESCRIPTION 'Subject',
    cc "string" WITH DESCRIPTION 'cc',
    bcc "string" WITH DESCRIPTION 'bcc',
    body "string" WITH DESCRIPTION 'Message body'
  ),
  FAMILY features WITH DESCRIPTION 'Derived features' (
    sentiment "float" WITH DESCRIPTION 'sentiment score'
  )
);
