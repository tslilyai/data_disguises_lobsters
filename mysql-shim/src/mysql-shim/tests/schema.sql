DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `moderator_user_id` int, `story_id` int, `user_id` int;

DROP TABLE IF EXISTS `stories` CASCADE;
CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int unsigned, `url` varchar(250) DEFAULT '';
    
DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `karma` int DEFAULT 0 NOT NULL, UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
