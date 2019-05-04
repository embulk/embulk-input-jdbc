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
