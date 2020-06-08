package gdpr

import "log"

func (as *AppServer) updateVoteRecords(votes []Vote, isUpload bool) Err {
	for _, newv := range votes {
		_, foundText := as.Id2Texts[newv.Id]
		v, foundVote := as.Id2VoteCounts[newv.Id]
		uv, foundUVote := as.U2Votes[newv.User][newv.Id]

		if !foundText && !isUpload {
			// the corresponding text doesn't exist and we're not uploading
			return ErrNotFound
		}

		// we add the vote regardless of whether the corresponding text exists,
		// which it may not if this is an upload
		if !foundVote {
			// text existed, but vote for text not allocated yet. add it
			// note that this will never happen under "retain" policies
			// because the user vote will have remained
			v = VoteCounts{}
			if newv.IsPos {
				v.Upvotes += 1
			} else {
				v.Downvotes += 1
			}
			as.Id2VoteCounts[newv.Id] = v
			as.U2Votes[newv.User][newv.Id] = newv
            as.DPrintf("Added vote %d", newv.Id)
		} else {
			// votecounts exist

			// check that vote is newer
			if foundUVote {
				if uv.Version >= newv.Version {
					as.DPrintf("Version %d >= %d of vote %d is greater! Not updating\n", 
                        uv.Version, newv.Version, newv.Id)
					continue
				} else if uv.IsPos != newv.IsPos {
					if newv.IsPos {
						v.Upvotes += 1
						v.Downvotes -= 1
					} else {
						v.Upvotes -= 1
						v.Downvotes += 1
					}
				}
			} else {
				// user vote doesn't exist; either never did, or was deleted
				switch as.policies.VotePolicy {
				case Retain:
					// we need to see if this vote was here before.
					invisId := newv.Id
					if invisV, found := as.InvisId2InvisIsPos[invisId]; found {
						if foundUVote {
							log.Fatalf("No user vote should exist for invis entry!")
						}
						if invisV != newv.IsPos {
							if newv.IsPos {
								v.Upvotes += 1
								v.Downvotes -= 1
							} else {
								v.Downvotes += 1
								v.Upvotes -= 1
							}
						}
						delete(as.InvisId2InvisIsPos, invisId)
					}
					break
				case Revoke, RevokeDelete:
					// add effects to votecount, this vote had been removed
					if newv.IsPos {
						v.Upvotes += 1
					} else {
						v.Downvotes += 1
					}
				}
			}
            as.Id2VoteCounts[newv.Id] = v
            as.U2Votes[newv.User][newv.Id] = newv
			as.DPrintf("Added vote %d", newv.Id)
		}
		// update the timestamp for this user
		as.U2Version[newv.User] = max(newv.Version, as.U2Version[newv.User])
	}
	return OK
}

func (as *AppServer) deleteVoteRecords(votes []Vote) Err {
	for _, newv := range votes {
		v, foundVote := as.Id2VoteCounts[newv.Id]
		uv, foundUserVote := as.U2Votes[newv.User][newv.Id]

		if !foundUserVote {
            as.DPrintf("Vote %d not found?", newv.Id)
			return ErrNotFound
		}

		// vote exists for this user
		// check that vote is at least as new
		if uv.Version > newv.Version {
			as.DPrintf("Version of vote %d is greater! Not updating\n", newv.Id)
			continue
		}

		// regardless of policy, we need to remove the copy of user data
		delete(as.U2Votes[newv.User], newv.Id)

		if !foundVote {
			// votecounts not allocated yet?
			log.Fatalf("User vote existed but not vote count?")
		} else {
			// handle depending on policy!
			switch as.policies.VotePolicy {
			case Retain:
				// keep effects, don't do anything to votecount
				// add invisId entry for vote
				invisId := newv.Id
				as.InvisId2InvisIsPos[invisId] = newv.IsPos
				break
			case Revoke, RevokeDelete:
				// remove effects from votecount
				if uv.IsPos {
					v.Upvotes -= 1
				} else {
					v.Downvotes -= 1
				}
			}
		}
        as.Id2VoteCounts[newv.Id] = v
        as.DPrintf("Deleted vote %d", newv.Id)

		// update the timestamp for this user
		as.U2Version[newv.User] = max(newv.Version, as.U2Version[newv.User])
	}
	return OK
}
