drop table if exists test1;

create table test1 (
    id   int not null,
    name varchar(8) not null,
    primary key(id)
);

insert into test1 values
(1, 'A'),
(2, 'B'),
(3, 'C'),
(4, 'D');

drop table if exists test2;

drop table if exists test3;

create table test3 (
    id         int not null,
    datetime1  datetime,
    datetime2  datetime,
    primary key(id)
);

insert into test3 values
(1, NULL, NULL),
(2, '2019-01-02 12:34:56', '2019-01-02 12:34:56'),
(3, '2018-12-31 23:59:59', '2018-12-31 23:59:59');
