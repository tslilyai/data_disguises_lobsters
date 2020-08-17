--
-- post revoke policy
---- change post and all comments to dangling
---- delete post content and ID
---- TODO: map from parent to all comments in comment chains?
--
UPDATE `ServicePost` SET 
    `dangling` = 1 
WHERE `parentId` IN (
    SELECT `postId` from `ServicePost`
    WHERE `userId` = ?
);

UPDATE `ServicePost`
SET 
    `userId` = NULL,
    `parentId` = NULL,
    `timestamp` = NULL,
    `data` = NULL,
    `postId` = NULL,
    `is_dangling` = 1,
WHERE `userId` = ?;

--
-- vote revoke policy: delete vote
--
DELETE * FROM `ServiceVote` WHERE `userId` = ?;
