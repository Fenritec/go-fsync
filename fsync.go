package fsync

import (
	"context"
	"path"
)

// New provider creates a new fast sync instance
func NewProvider(l LocalFS, r RemoteFS, d DecisionCallback, opts *Options) Provider {
	p := &provider{
		local:  l,
		remote: r,

		takeDecision: d,
		localChange:  make(chan LocalItem, 100),
		remoteChange: make(chan RemoteItem, 100),
	}

	if opts != nil {
		p.localFSDeleteNonEmptyFolder = opts.LocalFSDeleteNonEmptyFolder
		p.remoteFSDeleteNonEmptyFolder = opts.RemoteFSDeleteNonEmptyFolder
	}

	return p
}

// Checks the changes from the root "/"
func (p *provider) DoInitialSync(ctx context.Context) error {
	return p.CheckChanges(ctx, "/")
}

// Checks the changes from the requested relative path
func (p *provider) CheckChanges(ctx context.Context, rPath string) error {
	_, _, err := p.checkChanges(ctx, rPath, false, false, "", p.takeDecision)
	return err
}

func (p *provider) checkChanges(
	ctx context.Context,
	relativePath string,
	tryLocalDeletion,
	tryRemoteDeletion bool,
	keepOnlyChildRPath string,
	takeDecision DecisionCallback) (deletedLocally, deletedRemotely bool, err error) {
	select {
	case <-ctx.Done():
		return false, false, context.Canceled
	default:
		// Continue
	}

	lis, err := p.local.GetChildren(relativePath)
	if err != nil {
		return false, false, err
	}

	ris, err := p.remote.GetChildren(relativePath)
	if err != nil {
		return false, false, err
	}

	if keepOnlyChildRPath != "" {
		for i := len(lis) - 1; i >= 0; i-- {
			if lis[i].RelativePath != keepOnlyChildRPath {
				lis[i] = lis[len(lis)-1]
				lis = lis[:len(lis)-1]
			}
		}

		for i := len(ris) - 1; i >= 0; i-- {
			if ris[i].RelativePath != keepOnlyChildRPath {
				ris[i] = ris[len(ris)-1]
				ris = ris[:len(ris)-1]
			}
		}
	}

	exp, imp, con := p.classifyGroups(lis, ris)

	deleteLocals := []Decision{}
	deleteRemotes := []Decision{}

	// Exporting
	expDeleted := 0
	for _, e := range exp {
		if e.Commited == CommitedYes {
			if e.Dir {
				deletedLocally, _, err := p.checkChanges(ctx, e.RelativePath, true, false, "", takeDecision)
				if err != nil {
					return false, false, err
				}
				if deletedLocally {
					deleteLocals = append(deleteLocals, Decision{
						Flag:            DecisionDeleteLocal,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: "",
						RemoteIsDir:     true,
						Why:             newDecisionWhy(&e, nil),
					})
					expDeleted += 1
				}
			} else {
				deleteLocals = append(deleteLocals, Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
					RemoteIsDir:     e.Dir,
					Why:             newDecisionWhy(&e, nil),
				})
				expDeleted += 1
			}
		} else {
			if e.Commited == CommitedAwaitingRemoteDeletion {
				deleteLocals = append(deleteLocals, Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
					RemoteIsDir:     e.Dir,
					Why:             newDecisionWhy(&e, nil),
				})
			} else {
				if e.Dir {
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionCreateDirRemote,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
						RemoteIsDir:     e.Dir,
						Why:             newDecisionWhy(&e, nil),
					}); err != nil {
						return false, false, err
					}
					if _, _, err := p.checkChanges(ctx, e.RelativePath, false, false, "", takeDecision); err != nil {
						return false, false, err
					}
				} else {
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionUploadLocal,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
						RemoteIsDir:     e.Dir,
						Why:             newDecisionWhy(&e, nil),
					}); err != nil {
						return false, false, err
					}
				}
			}
		}
	}

	// Importing
	for _, i := range imp {
		if i.Dir {
			if err := takeDecision(ctx, Decision{
				Flag:            DecisionCreateDirLocal,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
				RemoteIsDir:     i.Dir,
				Why:             newDecisionWhy(nil, &i),
			}); err != nil {
				return false, false, err
			}
			if _, _, err := p.checkChanges(ctx, i.RelativePath, false, false, "", takeDecision); err != nil {
				return false, false, err
			}
		} else {
			if err := takeDecision(ctx, Decision{
				Flag:            DecisionDownloadRemote,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
				RemoteIsDir:     i.Dir,
				Why:             newDecisionWhy(nil, &i),
			}); err != nil {
				return false, false, err
			}
		}
	}

	// Managing conflicts (both side present)
	conDeleted := 0
	for _, c := range con {
		if c.li.Commited == CommitedYes {
			// If already commited on local side
			// Assume the server is right
			if c.li.Dir && !c.ri.Dir {
				// We have a file instead of a dir on the server
				// Check if we can delete the local dir and dowload the file locally
				deletedLocally, _, err := p.checkChanges(ctx, c.li.RelativePath, true, false, "", func(ctx context.Context, d Decision) error { return ctx.Err() })
				if err != nil {
					return false, false, err
				}
				d := Decision{
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
					RemoteIsDir:     c.ri.Dir,
					Why:             newDecisionWhy(&c.li, &c.ri),
				}
				if deletedLocally {
					d.Flag = DecisionDeleteLocalAndDownloadRemote
				} else {
					d.Flag = DecisionConflict
				}
				if err := takeDecision(ctx, d); err != nil {
					return false, false, err
				}
			} else if !c.li.Dir && c.li.Dir {
				// We have a dir instead of a file on the server
				// Delete the local file and create a dir locally
				if err := takeDecision(ctx, Decision{
					Flag:            DecisionDeleteLocalAndCreateDirLocal,
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
					RemoteIsDir:     c.ri.Dir,
					Why:             newDecisionWhy(&c.li, &c.ri),
				}); err != nil {
					return false, false, err
				}
				if _, _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, "", takeDecision); err != nil {
					return false, false, err
				}
			} else {
				// We have the same type on both side
				if c.li.Etag != c.ri.Etag {
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionDownloadRemote,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
						Why:             newDecisionWhy(&c.li, &c.ri),
					}); err != nil {
						return false, false, err
					}
				}
				// If it is a dir continue the inspection
				if c.li.Dir {
					if _, _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, "", takeDecision); err != nil {
						return false, false, err
					}
				}
			}
		} else {
			// Nothing is commited from local
			if !c.li.Dir && c.ri.Dir {
				// File locally and dir remotely
				if c.li.Commited == CommitedAwaitingRemoteDeletion {
					// Local item is a file awaiting deleting
					// We could delete it and request file download
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionCreateDirLocal,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
						Why:             newDecisionWhy(&c.li, &c.ri),
					}); err != nil {
						return false, false, err
					}
					if _, _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, "", takeDecision); err != nil {
						return false, false, err
					}
				} else {
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
						Why:             newDecisionWhy(&c.li, &c.ri),
					}); err != nil {
						return false, false, err
					}
				}
			} else if c.li.Dir && !c.ri.Dir {
				// Dir locally and file remotely
				if c.li.Commited == CommitedYes {
					// If local dir is not awaiting deletion
					deletedLocally, _, err := p.checkChanges(ctx, c.li.RelativePath, true, false, "", takeDecision)
					if err != nil {
						return false, false, err
					}
					if deletedLocally {
						if err := takeDecision(ctx, Decision{
							Flag:            DecisionDeleteLocalAndDownloadRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						}); err != nil {
							return false, false, err
						}
					} else {
						if err := takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						}); err != nil {
							return false, false, err
						}
					}
				} else if c.li.Commited == CommitedNo {
					// If local dir is not commited and there
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
						Why:             newDecisionWhy(&c.li, &c.ri),
					}); err != nil {
						return false, false, err
					}
				} else {
					// If local dir is awaiting remote deletion then download the file
					if err := takeDecision(ctx, Decision{
						Flag:            DecisionDownloadRemote,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
						Why:             newDecisionWhy(&c.li, &c.ri),
					}); err != nil {
						return false, false, err
					}
				}
			} else if c.li.Dir {
				// Both are directories
				if c.li.Commited == CommitedAwaitingRemoteDeletion {
					// Local dir wants to be deleted
					_, deletedRemotely, err := p.checkChanges(ctx, c.li.RelativePath, false, true, "", takeDecision)
					if err != nil {
						return false, false, err
					}
					if deletedRemotely {
						deleteRemotes = append(deleteRemotes, Decision{
							Flag:            DecisionDeleteRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: "",
							RemoteIsDir:     true,
							Why:             newDecisionWhy(&c.li, &c.ri),
						})
						conDeleted += 1
					}
				} else {
					if _, _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, "", takeDecision); err != nil {
						return false, false, err
					}
				}
			} else {
				// Both are files
				if c.li.Commited == CommitedAwaitingRemoteDeletion {
					if c.li.Etag == c.ri.Etag {
						// Etags are matching so we can delete
						deleteRemotes = append(deleteRemotes, Decision{
							Flag:            DecisionDeleteRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						})
						conDeleted += 1
					} else {
						if err := takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						}); err != nil {
							return false, false, err
						}
					}
				} else {
					if c.li.Etag == c.ri.Etag {
						// Assuming that li.Etag is containing the old etag
						// So we upload
						if err := takeDecision(ctx, Decision{
							Flag:            DecisionUploadLocal,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						}); err != nil {
							return false, false, err
						}
					} else {
						if err := takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
							Why:             newDecisionWhy(&c.li, &c.ri),
						}); err != nil {
							return false, false, err
						}
					}
				}
			}
		}
	}

	// If nothing to import and nothing to export
	// And there is no conflict
	// it means we can delete the folder
	if len(exp) == expDeleted && len(imp) == 0 && len(con) == conDeleted {
		deletedLocally = tryLocalDeletion
		deletedRemotely = tryRemoteDeletion
	}

	if !p.localFSDeleteNonEmptyFolder || !deletedLocally {
		for _, deleteLocal := range deleteLocals {
			if err := takeDecision(ctx, deleteLocal); err != nil {
				return false, false, err
			}
		}
	}

	if !p.remoteFSDeleteNonEmptyFolder || !deletedRemotely {
		for _, deleteRemote := range deleteRemotes {
			if err := takeDecision(ctx, deleteRemote); err != nil {
				return false, false, err
			}
		}
	}

	return
}

func (p *provider) classifyGroups(lis LocalItems, ris RemoteItems) (exp LocalItems, imp RemoteItems, con Conflicts) {
	exp = LocalItems{}
	imp = RemoteItems{}
	con = Conflicts{}

	// Export
	for _, li := range lis {
		presentRemotely := false
		for _, ri := range ris {
			if li.RelativePath == ri.RelativePath {
				presentRemotely = true
				break
			}
		}
		if !presentRemotely {
			exp = append(exp, li)
		}
	}

	// Import
	for _, ri := range ris {
		presentLocaly := false
		for _, li := range lis {
			if li.RelativePath == ri.RelativePath {
				presentLocaly = true
				break
			}
		}
		if !presentLocaly {
			imp = append(imp, ri)
		}
	}

	// Conflict
	for _, li := range lis {
		for _, ri := range ris {
			if li.RelativePath == ri.RelativePath {
				con = append(con, Conflict{li: li, ri: ri})
				break
			}
		}
	}

	return
}

// CheckDecision verifies if the decision is still ok after a certain amount of time
func (p *provider) CheckDecision(ctx context.Context, d Decision) (err error, ok bool) {
	var newDecision *Decision
	_, _, err = p.checkChanges(ctx, path.Dir(d.RelativePath), false, false, d.RelativePath, func(ctx context.Context, d2 Decision) error {
		if d.RelativePath == d2.RelativePath {
			newDecision = &d2
		}
		return nil
	})
	if err != nil {
		return err, false
	}

	if newDecision != nil &&
		newDecision.RelativePath == d.RelativePath &&
		newDecision.Flag == d.Flag &&
		newDecision.RemoteValidEtag == d.RemoteValidEtag &&
		newDecision.RemoteIsDir == d.RemoteIsDir {
		return nil, true
	}

	return nil, false
}
