drop table if exists input_hstore;

create extension if not exists hstore;

create table input_hstore (
    c1 hstore
);

insert into input_hstore (c1) values
('"a" => "b"')
;
