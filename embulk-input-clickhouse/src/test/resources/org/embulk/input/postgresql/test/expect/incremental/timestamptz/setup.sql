drop table if exists load;

create table load (
    time timestamptz(6) not null,
    note text
);

insert into load (time, note) values
('2016-11-02 01:00:01+0000', 'first'),
('2016-11-02 02:00:02+0000', 'first'),
('2016-11-02 03:00:03+0000', 'first'),
('2016-11-02 04:00:04+0000', 'first'),
('2016-11-02 04:00:05.111001+0000', 'first'),
('2016-11-02 04:00:05.222002+0000', 'first'),
('2016-11-02 04:00:05.333003+0000', 'first');

