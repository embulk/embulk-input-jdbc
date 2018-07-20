drop table if exists query_load;

create table query_load (
    num int not null,
    num2 int not null,
    note text
);

insert into query_load (num, num2, note) values
(3, 103, 'first'),
(4, 104, 'first'),
(2, 102, 'first'),
(1, 101, 'first');
