DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `moderator_user_id` int, `story_id` int, `user_id` int, `action` text, UNIQUE INDEX `user_id_mod_id`  (`moderator_user_id`, `user_id`));

DROP TABLE IF EXISTS `stories` CASCADE;
CREATE TABLE `stories` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `url` varchar(250) DEFAULT '', `is_moderated` int DEFAULT 0 NOT NULL);

DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `karma` int DEFAULT 0 NOT NULL);
