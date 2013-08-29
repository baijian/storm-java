CREATE TABLE `registerlog`(
    `id` int not null auto_increment primary key,
    `tablename` varchar(30) not null,
    `fieldnames` varchar(200) not null
);
