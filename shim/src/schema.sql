DROP TABLE IF EXISTS `comments` CASCADE;
CREATE TABLE `comments` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime, `short_id` varchar(10) DEFAULT '' NOT NULL, `story_id` int unsigned NOT NULL, `user_id` int unsigned NOT NULL, `parent_comment_id` int unsigned, `thread_id` int unsigned, `comment` mediumtext NOT NULL, `markeddown_comment` mediumtext, `is_deleted` tinyint(1) DEFAULT 0, `is_moderated` tinyint(1) DEFAULT 0, `is_from_email` tinyint(1) DEFAULT 0, fulltext INDEX `index_comments_on_comment`  (`comment`),  UNIQUE INDEX `short_id`  (`short_id`),  INDEX `story_id_short_id`  (`story_id`, `short_id`),  INDEX `thread_id`  (`thread_id`),  INDEX `index_comments_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `moderator_user_id` int, `story_id` int, `comment_id` int, `user_id` int, `action` mediumtext, `reason` mediumtext, `is_from_suggestions` tinyint(1) DEFAULT 0,  INDEX `index_moderations_on_created_at`  (`created_at`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `stories` CASCADE;
-- XXX: stories doesn't usually have an always-NULL column
CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `karma` int DEFAULT 0 NOT NULL, UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `votes` CASCADE;
CREATE TABLE `votes` (`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int unsigned NOT NULL, `story_id` int unsigned NOT NULL, `comment_id` int unsigned, `vote` tinyint NOT NULL, `reason` varchar(1),  INDEX `index_votes_on_comment_id`  (`comment_id`),  INDEX `user_id_comment_id`  (`user_id`, `comment_id`),  INDEX `user_id_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
--CREATE TABLE `comments`
--             (
--                          `id`                INT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
--                          `created_at`        DATETIME NOT NULL,
--                          `updated_at`        DATETIME,
--                          `short_id`          VARCHAR(10) DEFAULT '' NOT NULL,
--                          `story_id`          INT UNSIGNED NOT NULL,
--                          `user_id`           INT UNSIGNED NOT NULL,
--                          `parent_comment_id` INT UNSIGNED,
--                          `thread_id`         INT UNSIGNED,
--                          `comment` MEDIUMTEXT NOT NULL,
--                          `markeddown_comment` MEDIUMTEXT,
--                          `is_deleted`    TINYINT(1) DEFAULT 0,
--                          `is_moderated`  TINYINT(1) DEFAULT 0,
--                          `is_from_email` TINYINT(1) DEFAULT 0,
--                          FULLTEXT INDEX `index_comments_on_comment` (`comment`),
--                          UNIQUE INDEX `short_id` (`short_id`),
--                          INDEX `story_id_short_id` (`story_id`, `short_id`),
--                          INDEX `thread_id` (`thread_id`),
--                          INDEX `index_comments_on_user_id` (`user_id`)
--             )
--             engine=innodb DEFAULT charset=utf8mb4;
--
--CREATE TABLE `moderations`
--             (
--                          `id`                INT NOT NULL auto_increment PRIMARY KEY,
--                          `created_at`        DATETIME NOT NULL,
--                          `updated_at`        DATETIME NOT NULL,
--                          `moderator_user_id` INT,
--                          `story_id`          INT,
--                          `comment_id`        INT,
--                          `user_id`           INT, `action` MEDIUMTEXT,
--                          `reason` MEDIUMTEXT,
--                          `is_from_suggestions` TINYINT(1) DEFAULT 0,
--                          INDEX `index_moderations_on_created_at` (`created_at`)
--             )
--             engine=innodb DEFAULT charset=utf8mb4;
--
--CREATE TABLE `stories`
--             (
--                          `id`          INT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
--                          `always_null` INT,
--                          `created_at`  DATETIME,
--                          `user_id`     INT UNSIGNED,
--                          `url`         VARCHAR(250) DEFAULT '',
--                          `title`       VARCHAR(150) DEFAULT '' NOT NULL,
--                          `description` MEDIUMTEXT,
--                          `short_id`     VARCHAR(6) DEFAULT '' NOT NULL,
--                          `is_expired`   TINYINT(1) DEFAULT 0 NOT NULL,
--                          `is_moderated` TINYINT(1) DEFAULT 0 NOT NULL,
--                          `markeddown_description` MEDIUMTEXT,
--                          `story_cache` MEDIUMTEXT,
--                          `merged_story_id` INT,
--                          `unavailable_at`  DATETIME,
--                          `twitter_id`      VARCHAR(20),
--                          `user_is_author`  TINYINT(1) DEFAULT 0,
--                          INDEX `index_stories_on_created_at` (`created_at`),
--                          FULLTEXT INDEX `index_stories_on_description` (`description`),
--                          INDEX `is_idxes` (`is_expired`, `is_moderated`),
--                          INDEX `index_stories_on_is_expired` (`is_expired`),
--                          INDEX `index_stories_on_is_moderated` (`is_moderated`),
--                          INDEX `index_stories_on_merged_story_id` (`merged_story_id`),
--                          UNIQUE INDEX `unique_short_id` (`short_id`),
--                          FULLTEXT INDEX `index_stories_on_story_cache` (`story_cache`),
--                          FULLTEXT INDEX `index_stories_on_title` (`title`),
--                          INDEX `index_stories_on_twitter_id` (`twitter_id`),
--                          INDEX `url` (`url`(191)),
--                          INDEX `index_stories_on_user_id` (`user_id`)
--             )
--             engine=innodb DEFAULT charset=utf8mb4;
--CREATE TABLE `users`
--             (
--                          `id`       INT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
--                          `username` VARCHAR(50) collate utf8mb4_general_ci,
--                          `karma`    INT DEFAULT 0 NOT NULL,
--                          UNIQUE INDEX `username` (`username`)
--             )
--             engine=innodb DEFAULT charset=utf8;
--
--CREATE TABLE `votes`
--             (
--                          `id`         BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
--                          `user_id`    INT UNSIGNED NOT NULL,
--                          `story_id`   INT UNSIGNED NOT NULL,
--                          `comment_id` INT UNSIGNED,
--                          `vote`       TINYINT NOT NULL,
--                          `reason`     VARCHAR(1),
--                          INDEX `index_votes_on_comment_id` (`comment_id`),
--                          INDEX `user_id_comment_id` (`user_id`, `comment_id`),
--                          INDEX `user_id_story_id` (`user_id`, `story_id`)
--             )
--             engine=innodb DEFAULT charset=utf8;
