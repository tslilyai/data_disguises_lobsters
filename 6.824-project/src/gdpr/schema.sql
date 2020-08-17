-- 
-- USER SHARD TABLES 
-- 
DROP TABLE IF EXISTS `UserPost`;
CREATE TABLE `UserPost` (
  `postId` int(11) NOT NULL AUTO_INCREMENT,
  `parentId` int(11) DEFAULT NULL,
  `timestamp` bigint(11) DEFAULT NULL,
  `data` varbinary(8192) DEFAULT NULL,
  PRIMARY KEY (`postId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `UserVote`;
CREATE TABLE `Vote` (
  `voteId` int(11) NOT NULL AUTO_INCREMENT,
  `postId` int(11) NOT NULL,
  `timestamp` bigint(11) DEFAULT NULL,
  `positive` tinyInt(1) DEFAULT NULL,
  PRIMARY KEY (`voteId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 
-- SERVICE TABLES (user shard tables w/service-specific information)
-- 
DROP TABLE IF EXISTS `ServicePost`;
CREATE TABLE `ServicePosts` (
  `postId` int(11) DEFAULT NULL,
  `parentId` int(11) DEFAULT NULL,
  `timestamp` bigint(11) DEFAULT NULL,
  `data` varbinary(8192) DEFAULT NULL,
  
  `userId` int(11) DEFAULT NULL,
  `is_dangling` tinyInt(1) DEFAULT 0,
  `is_moderated` tinyInt(1) DEFAULT 0,
  PRIMARY KEY (`postId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ServiceVote`;
CREATE TABLE `ServicePosts` (
  `voteId` int(11) NOT NULL,
  `postId` int(11) NOT NULL,
  `timestamp` bigint(11) DEFAULT NULL,
  `positive` tinyInt(1) DEFAULT NULL,
  
  `userId` int(11) DEFAULT NULL,
  `is_dangling` tinyInt(1) DEFAULT 0,
  PRIMARY KEY (`voteId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 
-- View that groups together all information about non-dangling posts
-- 
CREATE VIEW `visible_post` AS
    SELECT 
        ServicePost.postId, 
        ServicePost.parentId, 
        ServicePost.timestamp,
        ServicePost.data
        ServicePost.userId, 
        ServicePost.is_moderated, 
        ServiceVote.upvotes - ServiceVote.downvotes AS score
    FROM `ServicePost` 
    LEFT JOIN `ServiceVote`
    WHERE ServiceVote.postId = ServicePost.postId 
        AND ServicePost.postId IS NOT NULL
        AND ServicePost.is_dangling = 0;

