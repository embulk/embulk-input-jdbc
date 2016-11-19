drop table if exists test1;

create table test1 (
    id  char(2),
    c1  tinyint,
    c2  smallint,
    c3  int,
    c4  bigint,
    c5  float,
    c6  double,
    c7  decimal(4,0),
    c8  decimal(20,2),
    c9  char(4),
    c10 varchar(4),
    c11 date,
    c12 datetime,
    c13 timestamp,
    c14 time,
    c15 datetime(6),
    primary key(id)
);

insert into test1 values(
    '10',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    '2015-06-04 23:45:06',
    null,
    null
);

insert into test1 values(
    '11',
    99,
    9999,
    -99999999,
    -9999999999999999,
    1.2345,
    1.234567890123,
    -1234,
    123456789012345678.12,
    '5678',
    'xy',
    '2015-06-04',
    '2015-06-04 12:34:56',
    '2015-06-04 23:45:06',
    '08:04:02',
    '2015-06-04 01:02:03.123456'
);

drop table if exists test2;

create table test2 (c1 bigint unsigned);

insert into test2 values(18446744073709551615);