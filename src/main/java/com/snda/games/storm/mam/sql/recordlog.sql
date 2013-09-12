CREATE TABLE `registerlog`(
    `id` int not null auto_increment primary key,
    `tablename` varchar(30) not null,
    `fieldsname` varchar(200) not null
);


insert into registerlog(`tablename`,`fieldsname`) values('actionlog', 'name,time,action,result');

CREATE TABLE `actionlog` (
    `id` int not null auto_increment primary key,
    `name` varchar(30) NOT NULL,
    `time` datetime NOT NULL,
    `action` varchar(30) NOT NULL,
    `result` varchar(30) NOT NULL
)