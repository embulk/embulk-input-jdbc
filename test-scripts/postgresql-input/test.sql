DROP TABLE IF EXISTS embulk_input;

CREATE TABLE embulk_input (
    id     int,
    num    decimal(12,2),
    str    char(8),
    varstr varchar(8),
    dt     date,
    dttm0  timestamp,
    dttm3  timestamp(3),
    primary key(ID)
);


INSERT INTO embulk_input VALUES(
  1,
  123.4,
  'test1',
  'TEST1',
  '2015-04-24',
  '2015-04-24 01:02:03',
  '2015-04-24 01:02:03.123'
);

INSERT INTO embulk_input VALUES(
  2,
  1234567890.12,
  'test9999',
  'TEST9999',
  '2015-12-31',
  '2015-12-31 23:59:59',
  '2015-12-31 23:59:59.999'
);

INSERT INTO embulk_input VALUES(
  3,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
);
