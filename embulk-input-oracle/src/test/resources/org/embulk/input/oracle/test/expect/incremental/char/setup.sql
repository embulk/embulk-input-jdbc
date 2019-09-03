drop table char_load;

create table char_load (
    name CHAR(2) not null,
    note VARCHAR2(10)
);

insert into char_load (name, note) values ('A3', 'first');
insert into char_load (name, note) values ('A4', 'first');
insert into char_load (name, note) values ('A2', 'first');
insert into char_load (name, note) values ('A1', 'first');

EXIT;
