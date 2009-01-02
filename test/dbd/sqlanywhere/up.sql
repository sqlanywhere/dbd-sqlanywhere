create table names (
    name varchar(255),
    age integer
);
---
insert into names (name, age) values ('Joe', 19);
---
insert into names (name, age) values ('Jim', 30);
---
insert into names (name, age) values ('Bob', 21);
---
create table precision_test (text_field varchar(20) primary key not null, integer_field decimal(2,1));
---
CREATE TABLE blob_test (name VARCHAR(30), data long binary);
---
create view view_names as select * from names;
---
create table boolean_test (num integer, mybool bit);
---
create table time_test (mytime time);
---
create table timestamp_test (mytimestamp timestamp);
---
create table bit_test (mybit bit);
---
create table field_types_test (foo integer not null primary key default 1);
---
create table db_specific_types_test (ts timestamp, dt date);
