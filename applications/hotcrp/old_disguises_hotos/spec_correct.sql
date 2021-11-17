CREATE VIEW PaperReviewPreference0 AS SELECT * FROM `PaperReviewPreference` WHERE NOT (`contactId` = 20)

CREATE VIEW PaperReviewPreference1 AS SELECT `paperRevPrefId` AS `paperRevPrefId`, `paperId` AS `paperId`, `contactId` AS `contactId`, preference AS preference, expertise AS expertise FROM `PaperReviewPreference0`

CREATE VIEW PaperReviewPreference2 AS SELECT `paperRevPrefId` AS `paperRevPrefId`, `paperId` AS `paperId`, preference AS preference, expertise AS expertise, 0 AS `contactId` FROM `PaperReviewPreference1`

CREATE TEMPORARY TABLE PaperReviewPreferenceTemp AS SELECT `paperRevPrefId` AS `paperRevPrefId`, `paperId` AS `paperId`, preference AS preference, expertise AS expertise, 0 AS `contactId` FROM `PaperReviewPreference2`

CREATE VIEW Capability0 AS SELECT * FROM `Capability` WHERE NOT (`contactId` = 20)

CREATE TEMPORARY TABLE CapabilityTemp AS SELECT `capabilityType` AS `capabilityType`, `contactId` AS `contactId`, `paperId` AS `paperId`, `timeExpires` AS `timeExpires`, salt AS salt, data AS data FROM `Capability0`

CREATE VIEW ContactInfo0 AS SELECT * FROM `ContactInfo` WHERE NOT (`contactId` = 20)

CREATE VIEW ContactInfo1 AS SELECT `contactId` AS `contactId`, `firstName` AS `firstName`, `lastName` AS `lastName`, `unaccentedName` AS `unaccentedName`, email AS email, `preferredEmail` AS `preferredEmail`, affiliation AS affiliation, phone AS phone, country AS country, password AS password, `passwordTime` AS `passwordTime`, `passwordUseTime` AS `passwordUseTime`, collaborators AS collaborators, `creationTime` AS `creationTime`, `updateTime` AS `updateTime`, `lastLogin` AS `lastLogin`, `defaultWatch` AS `defaultWatch`, roles AS roles, disabled AS disabled, `contactTags` AS `contactTags`, birthday AS birthday, gender AS gender, data AS data, `isGuise` AS `isGuise` FROM `ContactInfo0`

CREATE VIEW ContactInfo2 AS SELECT `contactId` AS `contactId`, `firstName` AS `firstName`, `lastName` AS `lastName`, `unaccentedName` AS `unaccentedName`, `preferredEmail` AS `preferredEmail`, affiliation AS affiliation, phone AS phone, country AS country, password AS password, `passwordTime` AS `passwordTime`, `passwordUseTime` AS `passwordUseTime`, collaborators AS collaborators, `creationTime` AS `creationTime`, `updateTime` AS `updateTime`, `lastLogin` AS `lastLogin`, `defaultWatch` AS `defaultWatch`, roles AS roles, disabled AS disabled, `contactTags` AS `contactTags`, birthday AS birthday, gender AS gender, data AS data, `isGuise` AS `isGuise`, 'anonymousLfVBOJAbb23N@secret.mail' AS email FROM `ContactInfo1` WHERE roles & 1 = 1 UNION SELECT * FROM `ContactInfo1` WHERE NOT (roles & 1 = 1)

CREATE TEMPORARY TABLE ContactInfoTemp AS SELECT `contactId` AS `contactId`, `firstName` AS `firstName`, `lastName` AS `lastName`, `unaccentedName` AS `unaccentedName`, `preferredEmail` AS `preferredEmail`, affiliation AS affiliation, phone AS phone, country AS country, password AS password, `passwordTime` AS `passwordTime`, `passwordUseTime` AS `passwordUseTime`, collaborators AS collaborators, `creationTime` AS `creationTime`, `updateTime` AS `updateTime`, `lastLogin` AS `lastLogin`, `defaultWatch` AS `defaultWatch`, roles AS roles, disabled AS disabled, `contactTags` AS `contactTags`, birthday AS birthday, gender AS gender, data AS data, `isGuise` AS `isGuise`, 'anonymousLfVBOJAbb23N@secret.mail' AS email FROM `ContactInfo2` WHERE roles & 1 = 1 UNION SELECT * FROM `ContactInfo2` WHERE NOT (roles & 1 = 1)

CREATE VIEW TopicInterest0 AS SELECT * FROM `TopicInterest` WHERE NOT (`contactId` = 20)

CREATE TEMPORARY TABLE TopicInterestTemp AS SELECT `topicInterestId` AS `topicInterestId`, `contactId` AS `contactId`, `topicId` AS `topicId`, interest AS interest FROM `TopicInterest0`

CREATE VIEW ActionLog0 AS SELECT `logId` AS `logId`, `destContactId` AS `destContactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `contactId` FROM `ActionLog` WHERE `contactId` = 20 UNION SELECT * FROM `ActionLog` WHERE NOT (`contactId` = 20)

CREATE VIEW ActionLog1 AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `destContactId` FROM `ActionLog0` WHERE `destContactId` = 20 UNION SELECT * FROM `ActionLog0` WHERE NOT (`destContactId` = 20)

CREATE VIEW ActionLog2 AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `destContactId` AS `destContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `trueContactId` FROM `ActionLog1` WHERE `trueContactId` = 20 UNION SELECT * FROM `ActionLog1` WHERE NOT (`trueContactId` = 20)

CREATE VIEW ActionLog3 AS SELECT `logId` AS `logId`, `destContactId` AS `destContactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `contactId` FROM `ActionLog2`

CREATE VIEW ActionLog4 AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `destContactId` FROM `ActionLog3`

CREATE VIEW ActionLog5 AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `destContactId` AS `destContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `trueContactId` FROM `ActionLog4`

CREATE VIEW ActionLog6 AS SELECT `logId` AS `logId`, `destContactId` AS `destContactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `contactId` FROM `ActionLog5`

CREATE VIEW ActionLog7 AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `trueContactId` AS `trueContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `destContactId` FROM `ActionLog6`

CREATE TEMPORARY TABLE ActionLogTemp AS SELECT `logId` AS `logId`, `contactId` AS `contactId`, `destContactId` AS `destContactId`, `paperId` AS `paperId`, timestamp AS timestamp, ipaddr AS ipaddr, action AS action, data AS data, 0 AS `trueContactId` FROM `ActionLog7`

CREATE VIEW PaperReviewRefused0 AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `timeRequested` AS `timeRequested`, `refusedBy` AS `refusedBy`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `requestedBy` FROM `PaperReviewRefused` WHERE `requestedBy` = 20 UNION SELECT * FROM `PaperReviewRefused` WHERE NOT (`requestedBy` = 20)

CREATE VIEW PaperReviewRefused1 AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `refusedBy` FROM `PaperReviewRefused0` WHERE `refusedBy` = 20 UNION SELECT * FROM `PaperReviewRefused0` WHERE NOT (`refusedBy` = 20)

CREATE VIEW PaperReviewRefused2 AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `timeRequested` AS `timeRequested`, `refusedBy` AS `refusedBy`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `requestedBy` FROM `PaperReviewRefused1`

CREATE VIEW PaperReviewRefused3 AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `refusedBy` FROM `PaperReviewRefused2`

CREATE VIEW PaperReviewRefused4 AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `timeRequested` AS `timeRequested`, `refusedBy` AS `refusedBy`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `requestedBy` FROM `PaperReviewRefused3`

CREATE TEMPORARY TABLE PaperReviewRefusedTemp AS SELECT `paperId` AS `paperId`, email AS email, `firstName` AS `firstName`, `lastName` AS `lastName`, affiliation AS affiliation, `contactId` AS `contactId`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRefused` AS `timeRefused`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, data AS data, reason AS reason, 0 AS `refusedBy` FROM `PaperReviewRefused4`

CREATE VIEW ReviewRating0 AS SELECT `ratingId` AS `ratingId`, `paperId` AS `paperId`, `reviewId` AS `reviewId`, rating AS rating, 0 AS `contactId` FROM `ReviewRating` WHERE `contactId` = 20 UNION SELECT * FROM `ReviewRating` WHERE NOT (`contactId` = 20)

CREATE VIEW ReviewRating1 AS SELECT `ratingId` AS `ratingId`, `paperId` AS `paperId`, `reviewId` AS `reviewId`, rating AS rating, 0 AS `contactId` FROM `ReviewRating0`

CREATE TEMPORARY TABLE ReviewRatingTemp AS SELECT `ratingId` AS `ratingId`, `paperId` AS `paperId`, `reviewId` AS `reviewId`, rating AS rating, 0 AS `contactId` FROM `ReviewRating1`

CREATE VIEW PaperComment0 AS SELECT `paperId` AS `paperId`, `commentId` AS `commentId`, `timeModified` AS `timeModified`, `timeNotified` AS `timeNotified`, `timeDisplayed` AS `timeDisplayed`, comment AS comment, `commentType` AS `commentType`, `replyTo` AS `replyTo`, ordinal AS ordinal, `authorOrdinal` AS `authorOrdinal`, `commentTags` AS `commentTags`, `commentRound` AS `commentRound`, `commentFormat` AS `commentFormat`, `commentOverflow` AS `commentOverflow`, 0 AS `contactId` FROM `PaperComment` WHERE `contactId` = 20 UNION SELECT * FROM `PaperComment` WHERE NOT (`contactId` = 20)

CREATE VIEW PaperComment1 AS SELECT `paperId` AS `paperId`, `commentId` AS `commentId`, `timeModified` AS `timeModified`, `timeNotified` AS `timeNotified`, `timeDisplayed` AS `timeDisplayed`, comment AS comment, `commentType` AS `commentType`, `replyTo` AS `replyTo`, ordinal AS ordinal, `authorOrdinal` AS `authorOrdinal`, `commentTags` AS `commentTags`, `commentRound` AS `commentRound`, `commentFormat` AS `commentFormat`, `commentOverflow` AS `commentOverflow`, 0 AS `contactId` FROM `PaperComment0`

CREATE TEMPORARY TABLE PaperCommentTemp AS SELECT `paperId` AS `paperId`, `commentId` AS `commentId`, `timeModified` AS `timeModified`, `timeNotified` AS `timeNotified`, `timeDisplayed` AS `timeDisplayed`, comment AS comment, `commentType` AS `commentType`, `replyTo` AS `replyTo`, ordinal AS ordinal, `authorOrdinal` AS `authorOrdinal`, `commentTags` AS `commentTags`, `commentRound` AS `commentRound`, `commentFormat` AS `commentFormat`, `commentOverflow` AS `commentOverflow`, 0 AS `contactId` FROM `PaperComment1`

CREATE VIEW PaperWatch0 AS SELECT * FROM `PaperWatch` WHERE NOT (`contactId` = 20)

CREATE VIEW PaperWatch1 AS SELECT `paperWatchId` AS `paperWatchId`, `paperId` AS `paperId`, `contactId` AS `contactId`, watch AS watch FROM `PaperWatch0`

CREATE VIEW PaperWatch2 AS SELECT `paperWatchId` AS `paperWatchId`, `paperId` AS `paperId`, watch AS watch, 0 AS `contactId` FROM `PaperWatch1`

CREATE TEMPORARY TABLE PaperWatchTemp AS SELECT `paperWatchId` AS `paperWatchId`, `paperId` AS `paperId`, watch AS watch, 0 AS `contactId` FROM `PaperWatch2`

CREATE VIEW PaperConflict0 AS SELECT * FROM `PaperConflict` WHERE NOT (`contactId` = 20)

CREATE TEMPORARY TABLE PaperConflictTemp AS SELECT `paperConflictId` AS `paperConflictId`, `paperId` AS `paperId`, `contactId` AS `contactId`, `conflictType` AS `conflictType` FROM `PaperConflict0`

CREATE VIEW PaperReview0 AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `contactId` FROM `PaperReview` WHERE `contactId` = 20 UNION SELECT * FROM `PaperReview` WHERE NOT (`contactId` = 20)

CREATE VIEW PaperReview1 AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `contactId` AS `contactId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `requestedBy` FROM `PaperReview0` WHERE `requestedBy` = 20 UNION SELECT * FROM `PaperReview0` WHERE NOT (`requestedBy` = 20)

CREATE VIEW PaperReview2 AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `contactId` FROM `PaperReview1`

CREATE VIEW PaperReview3 AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `contactId` AS `contactId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `requestedBy` FROM `PaperReview2`

CREATE VIEW PaperReview4 AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `requestedBy` AS `requestedBy`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `contactId` FROM `PaperReview3`

CREATE TEMPORARY TABLE PaperReviewTemp AS SELECT `paperId` AS `paperId`, `reviewId` AS `reviewId`, `contactId` AS `contactId`, `reviewToken` AS `reviewToken`, `reviewType` AS `reviewType`, `reviewRound` AS `reviewRound`, `timeRequested` AS `timeRequested`, `timeRequestNotified` AS `timeRequestNotified`, `reviewBlind` AS `reviewBlind`, `reviewModified` AS `reviewModified`, `reviewAuthorModified` AS `reviewAuthorModified`, `reviewSubmitted` AS `reviewSubmitted`, `reviewNotified` AS `reviewNotified`, `reviewAuthorNotified` AS `reviewAuthorNotified`, `reviewAuthorSeen` AS `reviewAuthorSeen`, `reviewOrdinal` AS `reviewOrdinal`, `reviewViewScore` AS `reviewViewScore`, `timeDisplayed` AS `timeDisplayed`, `timeApprovalRequested` AS `timeApprovalRequested`, `reviewEditVersion` AS `reviewEditVersion`, `reviewNeedsSubmit` AS `reviewNeedsSubmit`, `reviewWordCount` AS `reviewWordCount`, `reviewFormat` AS `reviewFormat`, `overAllMerit` AS `overAllMerit`, `reviewerQualification` AS `reviewerQualification`, novelty AS novelty, `technicalMerit` AS `technicalMerit`, `interestToCommunity` AS `interestToCommunity`, longevity AS longevity, grammar AS grammar, `likelyPresentation` AS `likelyPresentation`, `suitableForShort` AS `suitableForShort`, potential AS potential, fixability AS fixability, tfields AS tfields, sfields AS sfields, data AS data, 0 AS `requestedBy` FROM `PaperReview4`

CREATE VIEW Paper0 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `shepherdContactId` AS `shepherdContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `leadContactId` FROM `Paper` WHERE `leadContactId` = 20 UNION SELECT * FROM `Paper` WHERE NOT (`leadContactId` = 20)

CREATE VIEW Paper1 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `shepherdContactId` AS `shepherdContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `managerContactId` FROM `Paper0` WHERE `managerContactId` = 20 UNION SELECT * FROM `Paper0` WHERE NOT (`managerContactId` = 20)

CREATE VIEW Paper2 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `shepherdContactId` FROM `Paper1` WHERE `shepherdContactId` = 20 UNION SELECT * FROM `Paper1` WHERE NOT (`shepherdContactId` = 20)

CREATE VIEW Paper3 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `shepherdContactId` AS `shepherdContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `leadContactId` FROM `Paper2`

CREATE VIEW Paper4 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `shepherdContactId` AS `shepherdContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `managerContactId` FROM `Paper3`

CREATE VIEW Paper5 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `shepherdContactId` FROM `Paper4`

CREATE VIEW Paper6 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `shepherdContactId` AS `shepherdContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `leadContactId` FROM `Paper5`

CREATE VIEW Paper7 AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `shepherdContactId` AS `shepherdContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `managerContactId` FROM `Paper6`

CREATE TEMPORARY TABLE PaperTemp AS SELECT `paperId` AS `paperId`, title AS title, `authorInformation` AS `authorInformation`, abstract AS abstract, collaborators AS collaborators, `timeSubmitted` AS `timeSubmitted`, `timeWithdrawn` AS `timeWithdrawn`, `timeFinalSubmitted` AS `timeFinalSubmitted`, `timeModified` AS `timeModified`, `paperStorageId` AS `paperStorageId`, sha1 AS sha1, `finalPaperStorageId` AS `finalPaperStorageId`, blind AS blind, outcome AS outcome, `leadContactId` AS `leadContactId`, `managerContactId` AS `managerContactId`, `capVersion` AS `capVersion`, size AS size, mimetype AS mimetype, timestamp AS timestamp, `pdfFormatStatus` AS `pdfFormatStatus`, `withdrawReason` AS `withdrawReason`, `paperFormat` AS `paperFormat`, `dataOverflow` AS `dataOverflow`, 0 AS `shepherdContactId` FROM `Paper7`

