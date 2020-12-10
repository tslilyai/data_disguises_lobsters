use decor::policy::{GeneratePolicy, GhostColumnPolicy, EntityGhostPolicies, KeyRelationship, ApplicationPolicy};
use std::collections::HashMap;

fn get_ghost_policies() -> EntityGhostPolicies {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();
    ghost_policies 
}

fn get_hotcrp_policy() -> ApplicationPolicy {
    use decor::policy::DecorrelationPolicy::*;
    ApplicationPolicy{
        entity_type_to_decorrelate: "ContactInfo".to_string(), 
        ghost_policies : get_ghost_policies(), 
        edge_policies : vec![
            /*
             * ACTION LOG
             *  
             * Foreign keys:
             *      - contactId = the user performing the action
             *      - destContactId = user recipient of action 
             *      - trueContactId = either contactId or -1, indicating action was from link
             *      - paperId = paper acted upon
             * 
             *  When a user unsubscribes, all associated action log entries should no longer refer
             *  to this user. All parent-child (user->log entry) correlations should be broken.
             *
             *  However, the action log entry may still be correlated with *other* real users in
             *  the system; child-parent (log->user) correlations can remain. For example, if an
             *  unsubscribed user disables another real user account, the action log would reflect
             *  that a *ghost* user disabled the real user account. 
             *
             *  Action logs may link to a sensitive papers because the log records paper
             *  *deletions*. We can retain the (parent-child) link between a sensitive paper---a
             *  paper authored or reviewed by an unsubscribing user---and a log entry that records
             *  the paper's deletion. This is because any link between the paper and the
             *  unsubscribing user has already been decorrelated, and the delete action does not
             *  reveal information about who may have reviewed the paper. 
             *  
             *  Sensitive log entries in which the delete action is *performed* by the
             *  unsubscribing user can also retain links to paper IDs because the action has been
             *  decorrelated from the user by the policies specified above. 
             *
             *  Note that for all retained links, it does not make sense to add log entries for
             *  "noise" (to reduce sensitivity): a paper can only be deleted once, and adding more
             *  actions falsely "created by" or "sent to" other real users would reduce the log's
             *  utility.
             */
            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "destContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "trueContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            
            /*
             * CAPABILITY 
             *  
             * Foreign keys:
             *      - contactId = user granted the capabilty
             *      - paperId = paper for which the capability is granted
             * 
             *  When a user unsubscribes, all associated capabilities are decorrelated. A user's
             *  capabilities (and to which papers) can identify the user.
             *
             *  Capabilities associated with sensitive papers (authored or reviewed by
             *  unsubscribing users) can remain associated: if I have the capability to view a
             *  paper whose reviewer has unsubscribed, I should still be able to view the paper.
             */
            KeyRelationship{
                child: "Capability".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "Capability".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            
            /*
             * DELETED CONTACT INFO 
             *  
             * Foreign keys:
             *      - contactId = user deleted
             * 
             * XXX Shouldn't this information not be kept (when a user deletes their account, or when
             * accounts are merged)? it seems like it's being used for log
             * purposes?
             *
             * In any case, this should probably be removed when the specified contactId user
             * unsubscribes.
             */
            KeyRelationship{
                child: "DeletedContactInfo".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRemove,
                child_parent_decorrelation_policy: NoDecorRemove,
            },
 
            /*
             * DOCUMENT LINK
             *  
             * Foreign keys:
             *      - paperId = paper 
             *      - documentId = paper storage identifier
             *
             * XXX Lily's understanding of links: they connect together a paper with the actual
             * paper contents (PaperStorage) and the comments for the paper. I'm not exactly sure
             * what linkType does, other than it has to be between COMMENT_BEGIN and COMMENT_END
             *
             * Links between papers and their paper storage should be retained. 
             * Links between papers and paper comments should also be retained:
             *      We want the conversation thread of a particular paper to still be consistent
             *      even if one of the comment authors, reviewers, or paper authors unsubscribes.
             *      I believe that links between papers and paper comments reveals little
             *      information about the authors of either (unlike links between papers and
             *      collaborators)
             * 
             */
            KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "PaperComment".to_string(),
                column_name: "linkId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "PaperStorage".to_string(),
                column_name: "documentId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * FILTERED DOCUMENT 
             *  
             * Foreign keys:
             *      - inDocId = paper storage id to look up
             *      - outDocId = paper storage id found by filter search
             *
             * Just keep the links
             */
            KeyRelationship{
                child: "FilteredDocument".to_string(),
                parent: "PaperStorage".to_string(),
                column_name: "inDocId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "FilteredDocument".to_string(),
                parent: "PaperStorage".to_string(),
                column_name: "outDocId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * FORMULA 
             *  
             * Foreign keys:
             *      - createdBy = user who created the formula
             *
             * Decorrelate the user from her formulae
             */
            KeyRelationship{
                child: "Formula".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "createdBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain, // NA
            },

            /* 
             * MAIL LOG 
             * 
             * There are blobs for paperIds and recipients; it seems like an unsubscribed recipient
             * should be removed from log entries
             */
 
            /*
             * PAPER
             *  
             * Foreign keys:
             *      - leadContactId = lead author for paper
             *      - shepherdContactId = shepherd
             *      - managerContactId = managers of conference(?)
             *      - paperStorageId = actual paper file 
             *      - finalPaperStorageId = paper file for final version of paper
             *
             * Any unsubscribing user who is one of the parent contactIds should be decorrelated
             * from the paper (the paper's field value points to a ghost). The lead, sheperd, and manager
             * contact IDs do not leak information about who the others may be, so these links can
             * be retained.
             */
            KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "leadContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "shepherdContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "managerContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "Paper".to_string(),
                parent: "PaperStorage".to_string(),
                column_name: "paperStorageId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "Paper".to_string(),
                parent: "PaperStorage".to_string(),
                column_name: "finalPaperStorageId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PAPER COMMENT
             *  
             * Foreign keys:
             *      - paperId = paper commenting about
             *      - contactId = commenter 
             *
             * If commenters unsubscribe, they should be decorrelated from their comments; if they
             * are also authors or reviewers of the paper, note that those lead contact/reviewer
             * identifiers will also be decorrelated and replaced by ghosts.
             *
             * Comments should remain associated with the original paper.
             * 
             * Note that if a conflicting author has unsubscribed, the paper will have already been
             * decorrelated from that conflict, and the comment reviewer name can remain correlated.

             */
            KeyRelationship{
                child: "PaperComment".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperComment".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            },
 
            /*
             * PAPER CONFLICT
             *  
             * Foreign keys:
             *      - contactId = user with conflict (collaborator, reviewer conflicts) 
             *      - paperId = paper 
             *
             * If the conflict user (contactId) unsubscribes, the user is decorrelated from all their paper conflict
             * entries. 
             * 
             * However, it may be problematic for decorrelated paper conflicts to remain linked to
             * the paper: the paper's other attributes (such as other paper conflicts that link to this
             * paper) can re-identify the ghosted / anonymized user. 
             * 
             * Option 1: We decorrelate the paper conflict from the paper as well, linking to a
             * ghost paper instead. Observing the conflicts for the paper therefore hides that
             * there is a ghost user who also has a conflict with the paper. (This is the policy
             * specified below).
             *
             * Option 2: We introduce fake conflicts for this paper to add noise. This has the
             * problem of introducing spurious conflicts and messes with the semantics for
             * assigning reviewers, so this seems implausible.
             *
             * Option 3: We decorrelate *all* users who have paper conflicts for this paper from
             * the paper, so that all paper conflicts for the paper are ghost users.  (Note: DeCor
             * currently doesn't support this type of child->parent->child decorrelation). This
             * seems problematic because other collaborators will suddenly lose access to the
             * paper.
             *
             * If all paper conflict users have unsubscribed, then the paper will seemingly have no
             * paper conflicts; all ghost users will point to a ghost paper (which can be generated
             * as a clone of the original).
             */
            KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                // if the user is unsubscribing, decorrelate the user from this conflict
                parent_child_decorrelation_policy: Decor,
                // if the conflict has been decorrelated from a parent paper (and is sensitive), we
                // can keep the conflict link to the user because the transitive link to the paper
                // has been broken
                child_parent_decorrelation_policy: NoDecorRetain, 
            },

            KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                // if the conflict is sensitive, the associated paper should be decorrelated (thus
                // removing even a ghost trace of there having been this conflict)
                parent_child_decorrelation_policy: Decor,
                // if the paper is sensitive (e.g., the lead contact unsubscribes), the paper
                // should be decorrelated from its conflicts
                child_parent_decorrelation_policy: Decor,
            },
 
            /*
             * PAPER OPTION
             *  
             * Foreign keys:
             *      - paperId = paper with option
             * 
             * Paper options can be retained.
             */
            KeyRelationship{
                child: "PaperOption".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PAPER REVIEW
             *  
             * Foreign keys:
             *      - paperId = paper reviewed
             *      - contactId = reviewer
             *      - requestedBy = person assigning the review
             * 
             * Papers should be kept associated with their reviews.
             *
             * Unsubscribing reviewers (or requesters) should be decorrelated from their reviews.
             * However, if the paper is sensitive (the lead contact has been decorrelated), reviews
             * should still remain correlated.
             *
             * Note that if a conflicting author has unsubscribed, the paper will have already been
             * decorrelated from that conflict, and the review should remain correlated.
             */
            KeyRelationship{
                child: "PaperReview".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReview".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReview".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PAPER REVIEW PREFERENCE
             *  
             * Foreign keys:
             *      - paperId = paper requested to review 
             *      - contactId = requesting reviewer
             * 
             * Decorrelate unsubscribed contactIds from their preferences; papers can remain
             * linked. If the paper is sensitive, we can still keep requesting reviewer users
             * correlated.
             */
            KeyRelationship{
                child: "PaperReviewPreference".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReviewPreference".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            /*
             * PAPER REVIEW REFUSED 
             *  
             * Foreign keys:
             *      - paperId = paper  
             *      - contactId = user who was refused
             *      - requestedBy = user who requested the refusal
             *      - refusedBy = user who refused the paper review assignment
             *      - XXX email = email of user who was refused
             *      - refusedReviewId = paper review that was refused
             * 
             * Unsubscribing users should be decorrelated from their refused paper reviews
             * (similarly if they were the refuser), but papers and paper reviews can remain linked
             * to this record.
             *
             * It seems like the email address is used as a foreign key to identify the user as
             * well, so this should also be "decorrelated" (this email address identifies an
             * "abstract" entity)
             *
             * If the paper or paper review are sensitive (e.g., a paper author unsubscribed), the
             * refused review can still remain linked to its contacts. 
             *
             */
            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "PaperReview".to_string(),
                column_name: "refusedReviewId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "email".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "refusedBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
           
            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperStorage".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            // paperStorageId joined with DocumentLink.documentId
 
            /*
             * PaperTag
             *
             * Foreign keys:
             *      - paper = the paper with the tag
             *
             *  Papers can remain affiliated with their tags.
             *  
             *  XXX it seems like paper tags and paper tag annotations both contain the contact IDs
             *  of the creators in the tag contents?
             *
             *  XXX Paper tag annotations vs paper tags... what's the difference? PaperTagAnno
             *  seems to link an annotation ID to tags; the API allows users to delete, update, or insert
             *  new tag annotations?
             */
            KeyRelationship{
                child: "PaperTag".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PaperTopic
             *
             * Foreign keys:
             *      - paperId = paper
             *
             *  Papers can remain affiliated with their topics
             */
            KeyRelationship{
                child: "PaperTopic".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PaperWatch
             *
             * Foreign keys:
             *      - contactId = the user watching the paper
             *      - paperId = the paper
             *
             *  The user's watched papers should be decorrelated from the user.
             *
             *  XXX Other users watching the same paper could potentially leak information who a
             *  ghost user watching the same paper might be? (In the same way that paper conflicts
             *  might?) To handle this, we could decorrelate the paper watch entries with ghost
             *  user parents from their real papers.
             *
             */
            KeyRelationship{
                child: "PaperWatch".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperWatch".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
           
            /*
             * REVIEW RATING 
             *
             * Foreign keys:
             *      - contactId = the user giving the review rating 
             *      - requestedBy = the user who assigned this review
             *      - paperId = paper being reviewed
             *      - reviewId = review attached to rating
             *
             * The user rating or requesting the rating should be decorrelated from the rating.
             */
            KeyRelationship{
                child: "ReviewRating".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ReviewRating".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ReviewRating".to_string(),
                parent: "Review".to_string(),
                column_name: "reviewId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
       
            /*
             * REVIEW REQUEST
             *
             * Foreign keys:
             *      - email = the email of the user requesting the review
             *      - paperId = paper being requested to review
             *      - reviewId = paper review being requested
             *
             * The user requesting the review should be decorrelated from the request.
             *
             * It again seems like the email address is used as a foreign key to identify the user as
             * well, so this should also be "decorrelated" (this email address identifies an
             * "abstract" entity)
             */
            KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "email".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "PaperReview".to_string(),
                column_name: "reviewId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
        
            /* 
             * SETTINGS and TOPIC AREA have no parents...
             */

            /*
             * TOPIC INTEREST
             *
             * Foreign keys:
             *      - contactId = the user performing the action
             *      - topicArea
             *
             * Users should be decorrelated from topics of interest when they unsubscribe.
             *
             */
            KeyRelationship{
                child: "TopicInterest".to_string(),
                parent: "TopicArea".to_string(),
                column_name: "topicId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "TopicInterest".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            }
        ]
    }
}
