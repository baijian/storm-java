CREATE TABLE `url`(
    `id` int not null auto_increment primary key,
    `name` varchar(200) not null,
    `uri` varchar(100) not null
);

CREATE TABLE `alog` (
    `id` int not null auto_increment primary key,
    `url_id` int not null,
    `time` datetime not null,
    `count` int not null,
    KEY `url_time` (`url_id`,`time`)
);
