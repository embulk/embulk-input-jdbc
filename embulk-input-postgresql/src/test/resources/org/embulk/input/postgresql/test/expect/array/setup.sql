drop table if exists input_array;

create table input_array (
    c1 integer[],
    c2 text[][],
    c3 bool[][][]
);

insert into input_array (c1, c2, c3) values ('{1000, 2000, 3000, 4000}', '{{"red", "green"}, {"blue", "cyan"}}', '{{{true}}}');

insert into input_array (c1, c2, c3) values ('{5000, 6000, 7000, 8000}', '{{"yellow", "magenta"}, {"purple", "light,dark"}}', '{{{t,t},{f,f}},{{t,f},{f,t}}}');
