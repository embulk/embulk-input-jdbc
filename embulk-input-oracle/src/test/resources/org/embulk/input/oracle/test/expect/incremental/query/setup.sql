drop table query_load;

create table query_load (
    num NUMBER(10) not null,
    note VARCHAR2(10)
);

insert into query_load (num, note) values (3, 'first');
insert into query_load (num, note) values (4, 'first');
insert into query_load (num, note) values (2, 'first');
insert into query_load (num, note) values (1, 'first');

EXIT;
