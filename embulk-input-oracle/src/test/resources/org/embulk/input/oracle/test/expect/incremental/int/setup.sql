drop table int_load;

create table int_load (
    num NUMBER(10) not null,
    note VARCHAR2(10)
);

insert into int_load (num, note) values (3, 'first');
insert into int_load (num, note) values (4, 'first');
insert into int_load (num, note) values (2, 'first');
insert into int_load (num, note) values (1, 'first');

EXIT;
