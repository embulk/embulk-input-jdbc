drop table if exists load;

create table load (
    time timestamp without time zone not null,
    note text
);

insert into load (time, note) values
('2016-11-02 01:00:01', 'first'),
('2016-11-02 02:00:02', 'first'),
('2016-11-02 03:00:03', 'first'),
('2016-11-02 04:00:04', 'first'),
('2016-11-02 04:00:05.111001', 'first'),
('2016-11-02 04:00:05.222002', 'first'),
('2016-11-02 04:00:05.333003', 'first');

