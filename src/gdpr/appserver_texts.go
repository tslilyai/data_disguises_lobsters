package gdpr

import "log"

func (as *AppServer) deleteTextRecordAndChildren(text Text) {
	//  remove entries from any service-identifiable map
	// (anything with Ids rather than InvisIds)
	delete(as.Id2Texts, text.Id)
	delete(as.U2Ids[text.User], text.Id)

	for _, cid := range text.Children {
		if child, found := as.Id2Texts[cid]; found {
			as.deleteTextRecordAndChildren(child)
		}
	}
	if parent, found := as.Id2Texts[text.Parent]; found {
		parent.Children = findAndRemoveVal(parent.Children, text.Id)
        as.Id2Texts[text.Parent] = parent
	}
}

func (as *AppServer) revokeTextRecordAndChildren(text Text) string {
	// change all entries under this text record to invis entries
	// keep parent IDs for restoration, just in case
	invisIdBytes := as.encrypt(text.Id, text.User)
	children := []string{}

	for _, cid := range text.Children {
		if childText, found := as.Id2Texts[cid]; found {
			children = append(children, as.revokeTextRecordAndChildren(childText))
		}
	}
	as.InvisId2InvisTexts[invisIdBytes] = InvisText{
		InvisId:       invisIdBytes,
		InvisChildren:       children,
        TextCopy : text,
	}
	return string(invisIdBytes)
}

func (as *AppServer) deleteTextRecords(texts []Text) Err {
	for _, newtext := range texts {
		delete(as.U2Ids[newtext.User], newtext.Id)
		
        if text, found := as.Id2Texts[newtext.Id]; found {
			// the owner must be the same!
			if text.User != newtext.User {
                as.DPrintf("Delete: User %d: not equivalent to owner %d of text %d! Not authorized\n",
					text.User, newtext.User, newtext.Id)
				return ErrUnauthorized
			}

			// delete only if the new text is ... at least as old 
			if text.Version > newtext.Version {
				as.DPrintf("User %d: current version %d > %d, text %d is greater! Not updating\n",
					newtext.User, text.Version, newtext.Version, newtext.Id)
				continue
			}

			if text.Version > as.U2Version[newtext.User] {
                log.Fatalf("User %d: %d delete existing versions %d and %d inconsistent? %d texts", 
                    newtext.User, text.Id, text.Version, as.U2Version[newtext.User], len(texts))
			}

			// handle depending on policy!
			switch as.policies.TextPolicy {
			case Retain:
                // ensure that we authorize user if reuploads data
				text.Content = as.encrypt(text.Id, text.User) 
				text.User = -1
				text.Version = -1
				text.Id = -1
                as.Id2Texts[newtext.Id] = text
				break
			case Revoke:
				invisId := as.revokeTextRecordAndChildren(newtext)
				// erase all data of this top-level content
				// underlying comment chain is simply inaccessible
				invisText, found := as.InvisId2InvisTexts[invisId]
				if !found {
					log.Fatalf("Huh? Should have just been added")
				}
                invisText.TextCopy = Text{}
                as.InvisId2InvisTexts[invisId] = invisText

				// delete non-invis records of text and children
				as.deleteTextRecordAndChildren(newtext)
				break
			case RevokeDelete:
				as.deleteTextRecordAndChildren(newtext)
				break
			}
		} else if as.policies.TextPolicy == Revoke {
			// we didn't find the text, but we need to make sure we
			// erase properly in case it was currently invis
			invisId := as.revokeTextRecordAndChildren(newtext)
			if invisText, found := as.InvisId2InvisTexts[invisId]; found {
                invisText.TextCopy = Text{}
                as.InvisId2InvisTexts[invisId] = invisText
			}
		}
	    as.U2Version[newtext.User] = max(newtext.Version, as.U2Version[newtext.User])
	}
	return OK
}

func (as *AppServer) exposeTextRecordAndChildren(invisText InvisText) Err {
	parent, found := as.Id2Texts[invisText.TextCopy.Parent]
    if !found {
        as.DPrintf("Exposing content but parent not around?")
        return ErrNotFound
    } 
    
    // update parent's children list
    parent.Children = append(parent.Children, invisText.TextCopy.Id)
    as.Id2Texts[invisText.TextCopy.Parent] = parent
    
    // make this text visible
    as.Id2Texts[invisText.TextCopy.Id] = invisText.TextCopy
    as.U2Ids[invisText.TextCopy.User][invisText.TextCopy.Id] = true
    
    // change all entries under this text record to visible entries
    for _, invis_cid := range invisText.InvisChildren {
        if childText, found := as.InvisId2InvisTexts[invis_cid]; found {
            // if the child was actually deleted by its owner,
            // don't expose it or its children
            if childText.TextCopy.Version != -1 && childText.TextCopy.Parent != -1 {
                // we only want to attach children if parent exists
                // (which it should...)
                err := as.exposeTextRecordAndChildren(childText)
                if err != OK {
                    return err
                }
            }
        }
    }

    // remove from invis list!
    delete(as.InvisId2InvisTexts, invisText.InvisId)
    return OK
}

func (as *AppServer) updateTextRecords(texts []Text, isUpload bool) Err {
	for _, newText := range texts {

		// we found the text existing!
		if text, found := as.Id2Texts[newText.Id]; found {
            // the owner must be the same!
			if text.User != newText.User || (text.User == -1 && text.Content != as.encrypt(text.Id, newText.User)) {
                as.DPrintf("Update User %d: not equivalent to owner %d of text %d! Not authorized\n",
					text.User, newText.User, newText.Id)
				return ErrUnauthorized
			}

			// update only if the new text is ... newer
			if text.Version >= newText.Version {
				as.DPrintf("User %d: current version %d >= %d, text %s is greater! Not updating\n",
					newText.User, text.Version, newText.Version, newText.Id)
				continue
			}

			if text.Version > as.U2Version[newText.User] {
                log.Fatalf("User %d: update existing versions %d and %d inconsistent?", 
                    newText.User, text.Version, as.U2Version[newText.User])
			}

			// update the version and content
			text.Content = newText.Content
			text.Version = newText.Version
			// update user in case this was anonymized
			text.User = newText.User
            as.Id2Texts[newText.Id] = text
            as.U2Ids[newText.User][newText.Id] = true
		    as.U2Version[newText.User] = max(newText.Version, as.U2Version[newText.User])
		} else {
			// the text didn't exist
			// update parent's children list
			if parent, found := as.Id2Texts[newText.Parent]; found {
				parent.Children = append(parent.Children, newText.Id)
                as.Id2Texts[newText.Parent] = parent
			} else if newText.Parent != -1 && !isUpload {
				// can't find the parent! don't accept orphans if not uploading
				return ErrNotFound
			}
		    
            as.U2Version[newText.User] = max(newText.Version, as.U2Version[newText.User])
		
            // handle dependencies
			switch as.policies.TextPolicy {
			case Retain, RevokeDelete:
				// add the new text. it never existed for retain policies
                as.U2Ids[newText.User][newText.Id] = true
				as.Id2Texts[newText.Id] = newText

				// XXX should we pull children from user shards if policy was revokeAndDelete?
				break
			case Revoke:
				invisId := as.encrypt(newText.Id, newText.User)
				invisText, found := as.InvisId2InvisTexts[invisId]
				if found {
                    // update with new contents
					invisText.TextCopy = newText
                    if err := as.exposeTextRecordAndChildren(invisText); err != OK {
                        return err
                    }
                } else {
                    as.U2Ids[newText.User][newText.Id] = true
                    as.Id2Texts[newText.Id] = newText
                }
				break
			}
			as.DPrintf("Added text %d, user %d", newText.Id, newText.User)
        }
	}
	return OK
}
