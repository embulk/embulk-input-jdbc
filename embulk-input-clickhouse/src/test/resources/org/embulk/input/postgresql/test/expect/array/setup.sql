drop table if exists input_array;

create table input_array (
    c1 integer[],
    c2 text[][],
    c3 bool[][][],
    c4 decimal[]
);

insert into input_array (c1, c2, c3, c4) values ('{1000, 2000, 3000, 4000}', '{{"red", "green"}, {"blue", "cyan"}}', '{{{true}}}', '{1234567890}');

insert into input_array (c1, c2, c3, c4) values ('{5000, 6000, 7000, 8000}', '{{"yellow", "magenta"}, {"purple", "light,dark"}}', '{{{t,t},{f,f}},{{t,f},{f,t}}}', '{12345678901234567890}');

insert into input_array (c1, c2, c3, c4) values ('{1000}', '{{"\"", "{\\}", "{a,b}"}}', '{true}', '{12345678901234567890.1234567890}');
