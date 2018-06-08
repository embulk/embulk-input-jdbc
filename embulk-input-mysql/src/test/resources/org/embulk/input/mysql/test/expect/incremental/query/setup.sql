drop table if exists query_load;

create table query_load (
    num int not null,
    note text
);

insert into query_load (num, note) values
(3, 'first'),
(4, 'first'),
(2, 'first'),
(1, 'first');

