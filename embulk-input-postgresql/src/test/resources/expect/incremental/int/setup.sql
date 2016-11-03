drop table if exists int_load;

create table int_load (
    num int not null,
    note text
);

insert into int_load (num, note) values
(3, 'first'),
(4, 'first'),
(2, 'first'),
(1, 'first');

