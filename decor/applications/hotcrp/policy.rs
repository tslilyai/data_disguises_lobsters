use decor::policy::{GeneratePolicy, GhostColumnPolicy, ObjectGhostPolicies, EdgePolicy, MaskPolicy, ObjectName};
use std::collections::HashMap;
use std::rc::Rc;

fn get_pc_ghost_policies() -> ObjectGhostPolicies {
    let mut ghost_policies : ObjectGhostPolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("contactId".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("firstName".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("".to_string())));
    users_map.insert("lastName".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("".to_string())));
    users_map.insert("unaccentedName".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("".to_string())));
    users_map.insert("email".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("preferredEmail".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("affiliation".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("".to_string())));
    users_map.insert("phone".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("country".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("password".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("pass".to_string())));
    users_map.insert("passwordTime".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("passwordUseTime".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("collaborators".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("creationTime".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("updateTime".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("lastLogin".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("defaultWatch".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(2.to_string())));
    users_map.insert("roles".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    users_map.insert("disabled".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(1.to_string())));
    users_map.insert("contactTags".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("birthday".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("gender".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    users_map.insert("data".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
  
    ghost_policies.insert("users".to_string(), Rc::new(users_map));
    ghost_policies 
}

fn get_edge_policies() -> HashMap<ObjectName, Rc<Vec<EdgePolicy>>> {
    use decor::policy::EdgePolicyType::*;

    let mut edge_policies = HashMap::new();
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
    edge_policies.insert("ActionLog".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "destContactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "trueContactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
 
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
    edge_policies.insert("Capability".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
    
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
    edge_policies.insert("DeletedContactInfo".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Delete(0.0),
                cp_policy: Delete(0.0),
            },
        ])
    );
    
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
    edge_policies.insert("DocumentLink".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperComment".to_string(),
                column: "linkId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperStorage".to_string(),
                column: "documentId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );

    /*
     * FILTERED DOCUMENT 
     *  
     * Foreign keys:
     *      - inDocId = paper storage id to look up
     *      - outDocId = paper storage id found by filter search
     *
     * Just keep the links
     */
    edge_policies.insert("FilteredDocument".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "PaperStorage".to_string(),
                column: "inDocId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperStorage".to_string(),
                column: "outDocId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
    
    /*
     * FORMULA 
     *  
     * Foreign keys:
     *      - createdBy = user who created the formula
     *
     * Decorrelate the user from her formulae
     */
    edge_policies.insert("Formula".to_string(),
        Rc::new(vec![
        EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "createdBy".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain, // NA
            },
        ])
    );

    /* 
     * MAIL LOG 
     * 
     * There are blobs for paperIds and recipients; it seems like an unsubscribed recipient
     * should be removed from log entries
     *
     * XXX leaving this for now
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
    edge_policies.insert("Paper".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "leadContactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "shepherdContactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "managerContactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperStorage".to_string(),
                column: "paperStorageId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperStorage".to_string(),
                column: "finalPaperStorageId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
 
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
    edge_policies.insert("PaperComment".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ]),
    );
 
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
     * Option 1: Don't do anything. Leaks the most information.
     * 
     * Option 2: We delete the paper conflict from the paper as well. This can create spurious
     * conflicts (This is the policy
     * specified below).
     *
     * Option 3: We delete *all* users who have paper conflicts for this paper from
     * the paper, so that all paper conflicts for the paper are ghost users. This
     * seems problematic because other collaborators will suddenly lose access to the
     * paper, and reviewers may be wrongly assigned papers
     *
     * If all paper conflict users have unsubscribed, then the paper will seemingly have no
     * paper conflicts; all ghost users will point to a ghost paper (which can be generated
     * as a clone of the original).
     */
    edge_policies.insert("PaperConflict".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                // if the user is unsubscribing, decorrelate the user from this conflict
                pc_policy: Decorrelate(0.0),
                // if the conflict has been decorrelated from a parent paper (and is sensitive), we
                // can keep the conflict link to the user because the transitive link to the paper
                // has been broken
                cp_policy: Retain, 
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                // if the conflict is sensitive, we should remove it, preventing there from being
                // even a ghost trace
                pc_policy: Delete(0.0),
                // if the paper is sensitive (e.g., the lead contact unsubscribes), we keep the
                // conflicts of the paper because we want to keep the paper information 
                cp_policy: Retain,
            },
        ])
    );

    /*
     * PAPER OPTION
     *  
     * Foreign keys:
     *      - paperId = paper with option
     * 
     * Paper options can be retained.
     */
    edge_policies.insert("PaperOption".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
 
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
     edge_policies.insert("PaperReview".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "requestedBy".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ])
    );

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
     edge_policies.insert("PaperReviewPreference".to_string(),
        Rc::new(vec![
             EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ])
    );

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
     * "abstract" object)
     *
     * If the paper or paper review are sensitive (e.g., a paper author unsubscribed), the
     * refused review can still remain linked to its contacts. 
     *
     */
    edge_policies.insert("PaperReviewRefused".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperReview".to_string(),
                column: "refusedReviewId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "email".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "requestedBy".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "refusedBy".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );

    /*
     * PaperStorage
     */
    edge_policies.insert("PaperStorage".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            // paperStorageId joined with DocumentLink.documentId
        ])
    );
 
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
    edge_policies.insert("PaperTag".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
 
    /*
     * PaperTopic
     *
     * Foreign keys:
     *      - paperId = paper
     *
     *  Papers can remain affiliated with their topics
     */
    edge_policies.insert("PaperTopic".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
 
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
    edge_policies.insert("PaperWatch".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ])
    );
           
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
    edge_policies.insert("ReviewRating".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Review".to_string(),
                column: "reviewId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
       
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
     * "abstract" object)
     */
    edge_policies.insert("ReviewRequest".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "email".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "Paper".to_string(),
                column: "paperId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "PaperReview".to_string(),
                column: "reviewId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
        
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
    edge_policies.insert("TopicInterest".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "TopicArea".to_string(),
                column: "topicId".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },

            EdgePolicy{
                parent: "ContactInfo".to_string(),
                column: "contactId".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            }
        ])
    );
    edge_policies
}

pub fn get_hotcrp_policy() -> MaskPolicy {
    MaskPolicy{
        unsub_object_type: "ContactInfo".to_string(), 
        pc_ghost_policies : get_pc_ghost_policies(), 
        cp_ghost_policies : HashMap::new(), 
        edge_policies : get_edge_policies(),
    }
}
