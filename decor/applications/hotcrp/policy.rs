use decor::policy::{GeneratePolicy, GhostColumnPolicy, EntityGhostPolicies, KeyRelationship, ApplicationPolicy};
use std::collections::HashMap;

fn get_ghost_policies() -> EntityGhostPolicies {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();
    ghost_policies 
}

fn get_prestashop_policy() -> ApplicationPolicy {
    use decor::policy::DecorrelationPolicy::*;
    ApplicationPolicy{
        entity_type_to_decorrelate: "ContaactInfo".to_string(), 
        ghost_policies : get_ghost_policies(), 
        edge_policies : vec![
            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactId".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactId".to_string(),
                column_name: "destContactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            KeyRelationship{
                child: "ActionLog".to_string(),
                parent: "ContactId".to_string(),
                column_name: "trueContactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

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

           KeyRelationship{
                child: "DeletedContactInfo".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "DocumentLink".to_string(),
                column_name: "linkId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "DocumentLink".to_string(),
                parent: "DocumentLink".to_string(),
                column_name: "documentId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
KeyRelationship{
                child: "FilteredDocument".to_string(),
                parent: "DocumentLink".to_string(),
                column_name: "inDocId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "FilteredDocument".to_string(),
                parent: "DocumentLink".to_string(),
                column_name: "outDocId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "Formula".to_string(),
                parent: "ContactId".to_string(),
                column_name: "createdBy".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "MailLog".to_string(),
                parent: "ContactId".to_string(),
                column_name: "createdBy".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

            // maillog has blobs for paperIds and recipients; since it's a log, just leave it?
            // (might include nonexistent recipients and IDs)

           KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "leadContactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "shepherdContactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

           KeyRelationship{
                child: "Paper".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "managerContactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

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
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },

          KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "ContactInfo".to_string(),
                column_name: "contactId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
          },

         KeyRelationship{
                child: "PaperConflict".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

        KeyRelationship{
                child: "PaperOption".to_string(),
                parent: "Paper".to_string(),
                column_name: "paperId".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
        },

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
