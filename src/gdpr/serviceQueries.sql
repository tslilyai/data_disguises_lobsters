-- READ Query
SELECT * FROM `visible_posts` WHERE postId = ?;

-- Insert or update post Query
INSERT INTO `ServicePost(
        postId,
        parentId,
        timestamp,
        data,
        userId
    ) VALUES (?,?,?,?,?)
    ON DUPLICATE KEY UPDATE(
        timestamp=IF(timestamp < ?, ?, timestamp),
        data=IF(timestamp < ?, ?, data)
    );

-- Insert or update vote Query
INSERT INTO `ServiceVote(
        voteId,
        postId,
        timestamp,
        isPos,
        userId
    ) VALUES (?,?,?,?,?)
    ON DUPLICATE KEY UPDATE(
        timestamp=IF(timestamp < ?, ?, timestamp),
        isPos=IF(timestamp < ?, ?, data)
    );

-- Delete vote Query
-- It seems like any service will change the vote count if the user explicitly deletes
-- otherwise a user could always game the system
DELETE * FROM `ServiceVote` WHERE `userId` = ?;

-- Delete post Query
-- Same semantics as if reuploading was alowed? 
---- For revoke, can permanantly "hide" children comments
---- Should still just mark TODO need to make children either dangling or not depending on deletion policy
DELETE * FROM `ServicePost` WHERE `userId` = ?;
