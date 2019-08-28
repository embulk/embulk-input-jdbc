drop table if exists int_load;

create table int_load (
    name char(2) not null,
    note text
);

insert into int_load (name, note) values
('A3', 'first'),
('A4', 'first'),
('A2', 'first'),
('A1', 'first');
