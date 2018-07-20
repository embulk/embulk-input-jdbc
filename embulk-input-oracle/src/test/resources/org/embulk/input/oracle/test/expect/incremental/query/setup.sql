drop table query_load;

create table query_load (
    num NUMBER(10) not null,
    num2 NUMBER(10) not null,
    note VARCHAR2(10)
);

insert into query_load (num, num2, note) values (3, 103, 'first');
insert into query_load (num, num2, note) values (4, 104, 'first');
insert into query_load (num, num2, note) values (2, 102, 'first');
insert into query_load (num, num2, note) values (1, 101, 'first');

EXIT;
