drop table if exists ts_load;

create table ts_load (
    time timestamp(6) not null,
    note text
);

insert into ts_load (time, note) values
('2016-11-02 01:00:01', 'first'),
('2016-11-02 02:00:02', 'first'),
('2016-11-02 03:00:03', 'first'),
('2016-11-02 04:00:04', 'first'),
('2016-11-02 04:00:05.111001', 'first'),
('2016-11-02 04:00:05.222002', 'first'),
('2016-11-02 04:00:05.333003', 'first');

