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
                parent: "ContactId".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactId".to_string(),
                column_name: "destContactId".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactId".to_string(),
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
                parent: "ContactId".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
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
                parent: "ContactId".to_string(),
                column_name: "createdBy".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
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
             * If all paper conflicts have unsubscribed, then the paper will seemingly have no
             * paper conflicts; all ghost users will point to a ghost paper (which can be generated
             * as a clone of the original).
             */
            KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                // if the conflict is sensitive, the associated user should be decorrelated and
                // vice versa
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor, 
            },

            KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                // if the conflict is sensitive, the associated paper should be decorrelated and
                // vice versa
                parent_child_decorrelation_policy: Decor,
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
             * TODO
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
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReview".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
 
            /*
             * PAPER REVIEW PREFERENCE
             *  
             * Foreign keys:
             *      - paperId = paper requested to review 
             *      - contactId = requesting reviewer
             * 
             * TODO
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
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            /*
             * PAPER REVIEW REFUSED 
             *  
             * Foreign keys:
             *      - paperId = paper  
             *      - contactId = user who was refused
             *      - refusedReviewId = paper review that was refused
             *      - requestedBy = user who requested the refusal
             *      - XXX email = email of user who was refused
             * 
             * TODO
             */
            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "PaperReviewRefused".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
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
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
        
            // TODO email isn't the primary index into ContactInfo, but it's
            // used as the foreign key
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
                column_name: "refusedBy".to_string(),
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

        KeyRelationship{
                child: "PaperTag".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

        // I'm not sure what ``annoId'' refers to; an anonymous contactId?
        // TODO difference between PaperTag and PaperTagAnno; seems like tags are generated from
        // contactIDs?
        KeyRelationship{
                child: "PaperTagAnno".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "annoId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

        KeyRelationship{
                child: "PaperTopic".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

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
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

        KeyRelationship{
                child: "ReviewRating".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
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

        KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "requestedBy".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },
       
        // TODO email isn't the primary index into ContactInfo, but it's
        // used as the foreign key
        KeyRelationship{
                child: "ReviewRequest".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "email".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
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
                parent: "Review".to_string(),
                column_name: "reviewId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },
        
        // settings and topic area have no children...

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
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        }]
    }
}
