drop table if exists input_bytea;

create table input_bytea (
    c1 bytea
);

insert into input_bytea (c1) values
(decode('YWJjNDU2', 'base64'))
;
