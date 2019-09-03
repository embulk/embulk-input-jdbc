drop table if exists char_load;

create table char_load (
    name char(2) not null,
    note text
);

insert into char_load (name, note) values
('A3', 'first'),
('A4', 'first'),
('A2', 'first'),
('A1', 'first');

