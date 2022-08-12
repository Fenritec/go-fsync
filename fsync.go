package fsync

import (
	"context"
	"path"
)

// New provider creates a new fast sync instance
func NewProvider(l LocalFS, r RemoteFS, d DecisionCallback) Provider {
	p := &provider{
		local:  l,
		remote: r,

		takeDecision: d,
		localChange:  make(chan LocalItem, 100),
		remoteChange: make(chan RemoteItem, 100),
	}

	return p
}

// Checks the changes from the root "/"
func (p *provider) DoInitialSync(ctx context.Context) error {
	return p.CheckChanges(ctx, "/")
}

// Checks the changes from the requested relative path
func (p *provider) CheckChanges(ctx context.Context, rPath string) error {
	_, err := p.checkChanges(ctx, rPath, false, false, p.takeDecision)
	return err
}

func (p *provider) checkChanges(ctx context.Context, relativePath string, localDeleted, remoteDeleted bool, takeDecision DecisionCallback) (deleted bool, err error) {
	select {
	case <-ctx.Done():
		return false, context.Canceled
	default:
		// Continue
	}

	lis, err := p.local.GetChildren(relativePath)
	if err != nil {
		return false, err
	}

	ris, err := p.remote.GetChildren(relativePath)
	if err != nil {
		return false, err
	}

	exp, imp, con := p.classifyGroups(lis, ris)

	// Exporting
	expDeleted := 0
	for _, e := range exp {
		if e.Commited == CommitedYes {
			if e.Dir {
				deleted, err := p.checkChanges(ctx, e.RelativePath, true, false, takeDecision)
				if err != nil {
					return false, err
				}
				if deleted {
					expDeleted += 1
				}
			} else {
				takeDecision(ctx, Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
					RemoteIsDir:     e.Dir,
				})
				expDeleted += 1
			}
		} else {
			if e.Commited == CommitedAwaitingDeletion {
				takeDecision(ctx, Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
					RemoteIsDir:     e.Dir,
				})
			} else {
				if e.Dir {
					takeDecision(ctx, Decision{
						Flag:            DecisionCreateDirRemote,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
						RemoteIsDir:     e.Dir,
					})
					if _, err := p.checkChanges(ctx, e.RelativePath, false, false, takeDecision); err != nil {
						return false, err
					}
				} else {
					takeDecision(ctx, Decision{
						Flag:            DecisionUploadLocal,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
						RemoteIsDir:     e.Dir,
					})
				}
			}
		}
	}

	// Importing
	for _, i := range imp {
		if i.Dir {
			takeDecision(ctx, Decision{
				Flag:            DecisionCreateDirLocal,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
				RemoteIsDir:     i.Dir,
			})
			if _, err := p.checkChanges(ctx, i.RelativePath, false, false, takeDecision); err != nil {
				return false, err
			}
		} else {
			takeDecision(ctx, Decision{
				Flag:            DecisionDownloadRemote,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
				RemoteIsDir:     i.Dir,
			})
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
				// Delete the local dir and dowload the file locally
				takeDecision(ctx, Decision{
					Flag:            DecisionDeleteLocalAndDownloadRemote,
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
					RemoteIsDir:     c.ri.Dir,
				})
			} else if !c.li.Dir && c.li.Dir {
				// We have a dir instead of a file on the server
				// Delete the local file and create a dir locally
				takeDecision(ctx, Decision{
					Flag:            DecisionDeleteLocalAndCreateDirLocal,
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
					RemoteIsDir:     c.ri.Dir,
				})
				if _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, takeDecision); err != nil {
					return false, err
				}
			} else {
				// We have the same type on both side
				if c.li.Etag != c.ri.Etag {
					takeDecision(ctx, Decision{
						Flag:            DecisionDownloadRemote,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
					})
				}
				// If it is a dir continue the inspection
				if c.li.Dir {
					if _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, takeDecision); err != nil {
						return false, err
					}
				}
			}
		} else {
			// Nothing is commited from local
			if !c.li.Dir && c.ri.Dir {
				// File locally and dir remotely
				if c.li.Commited == CommitedAwaitingDeletion {
					// Local item is a file awaiting deleting
					// We could delete it and request file download
					takeDecision(ctx, Decision{
						Flag:            DecisionCreateDirLocal,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
					})
					if _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, takeDecision); err != nil {
						return false, err
					}
				} else {
					takeDecision(ctx, Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
					})
				}
			} else if c.li.Dir && !c.li.Dir {
				// Dir locally and file remotely
				if c.li.Commited != CommitedAwaitingDeletion {
					deleted, err := p.checkChanges(ctx, c.li.RelativePath, false, false, takeDecision)
					if err != nil {
						return false, err
					}
					if deleted {
						takeDecision(ctx, Decision{
							Flag:            DecisionDownloadRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
					} else {
						takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
					}
				} else {
					takeDecision(ctx, Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
						RemoteIsDir:     c.ri.Dir,
					})
				}
			} else if c.li.Dir {
				// Both are directories
				if c.li.Commited == CommitedAwaitingDeletion {
					// Local dir wants to be deleted
					if _, err := p.checkChanges(ctx, c.li.RelativePath, false, true, takeDecision); err != nil {
						return false, err
					}
				} else {
					if _, err := p.checkChanges(ctx, c.li.RelativePath, false, false, takeDecision); err != nil {
						return false, err
					}
				}
			} else {
				// Both are files
				if c.li.Commited == CommitedAwaitingDeletion {
					if c.li.Etag == c.ri.Etag {
						// Etags are matching so we can delete
						takeDecision(ctx, Decision{
							Flag:            DecisionDeleteRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
						conDeleted += 1
					} else {
						takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
					}
				} else {
					if c.li.Etag == c.ri.Etag {
						// Assuming that li.Etag is containing the old etag
						// So we upload
						takeDecision(ctx, Decision{
							Flag:            DecisionUploadLocal,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
					} else {
						takeDecision(ctx, Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
							RemoteIsDir:     c.ri.Dir,
						})
					}
				}
			}
		}
	}

	// If nothing to import and nothing to export
	// And there is no conflict
	// it means we can delete the folder
	if len(exp) == expDeleted && len(imp) == 0 && len(con) == conDeleted {
		if localDeleted {
			takeDecision(ctx, Decision{
				Flag:            DecisionDeleteLocal,
				RelativePath:    relativePath,
				RemoteValidEtag: "",
				RemoteIsDir:     true,
			})
			return true, nil
		}
		if remoteDeleted {
			takeDecision(ctx, Decision{
				Flag:            DecisionDeleteRemote,
				RelativePath:    relativePath,
				RemoteValidEtag: "",
				RemoteIsDir:     true,
			})
			return true, nil
		}
	}

	return false, nil
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
	var ri *RemoteItem

	ris, err := p.remote.GetChildren(path.Base(d.RelativePath))
	if err != nil {
		return err, false
	} else {
		for _, lRi := range ris {
			if lRi.RelativePath == d.RelativePath {
				ri = &lRi
				break
			}
		}
	}

	switch d.Flag {
	case DecisionCreateDirLocal:
		// No impact as if remote changes this can be downloader later
		return nil, true
	case DecisionCreateDirRemote:
		if ri == nil {
			return nil, true
		}
		return nil, ri.Dir
	case DecisionDownloadRemote:
		// No impact as remote change was already there
		return nil, true
	case DecisionDeleteLocal:
		// No impact as if remote changes this can be downloader later
		return nil, true
	case DecisionConflict:
		// Conflict must be handled by the middleware
		return nil, true
	case DecisionDeleteRemote:
		if ri == nil {
			return nil, true
		}

		// Both files
		if !d.RemoteIsDir && !ri.Dir {
			return nil, ri.Etag == d.RemoteValidEtag
		} else if d.RemoteIsDir != ri.Dir {
			return nil, false
		} else {
			deleted, err := p.checkChanges(ctx, d.RelativePath, false, true, DecisionCallback(func(context.Context, Decision) {}))
			if err != nil {
				return err, false
			}
			return nil, deleted
		}
	case DecisionDeleteLocalAndCreateDirLocal:
		fallthrough
	case DecisionDeleteLocalAndDownloadRemote:
		fallthrough
	case DecisionUploadLocal:
		if ri == nil {
			return nil, true
		}
		return nil, ri.Etag == d.RemoteValidEtag
	}
	return nil, false
}
