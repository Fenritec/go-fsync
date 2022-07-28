package fsync

import "path"

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
func (p *provider) DoInitialSync() error {
	return p.CheckChanges("/")
}

// Checks the changes from the requested relative path
func (p *provider) CheckChanges(rPath string) error {
	_, err := p.checkChanges(rPath, false, false)
	return err
}

func (p *provider) checkChanges(relativePath string, localDeleted, remoteDeleted bool) (deleted bool, err error) {
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
				deleted, err := p.checkChanges(e.RelativePath, true, false)
				if err != nil {
					return false, err
				}
				if deleted {
					expDeleted += 1
				}
			} else {
				p.takeDecision(Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
				})
				expDeleted += 1
			}
		} else {
			if e.Commited == CommitedAwaitingDeletion {
				p.takeDecision(Decision{
					Flag:            DecisionDeleteLocal,
					RelativePath:    e.RelativePath,
					RemoteValidEtag: e.Etag,
				})
			} else {
				if e.Dir {
					p.takeDecision(Decision{
						Flag:            DecisionCreateDirRemote,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
					})
					if _, err := p.checkChanges(e.RelativePath, false, false); err != nil {
						return false, err
					}
				} else {
					p.takeDecision(Decision{
						Flag:            DecisionUploadLocal,
						RelativePath:    e.RelativePath,
						RemoteValidEtag: e.Etag,
					})
				}
			}
		}
	}

	// Importing
	for _, i := range imp {
		if i.Dir {
			p.takeDecision(Decision{
				Flag:            DecisionCreateDirLocal,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
			})
			if _, err := p.checkChanges(i.RelativePath, false, false); err != nil {
				return false, err
			}
		} else {
			p.takeDecision(Decision{
				Flag:            DecisionDownloadRemote,
				RelativePath:    i.RelativePath,
				RemoteValidEtag: i.Etag,
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
				p.takeDecision(Decision{
					Flag:            DecisionDeleteLocalAndDownloadRemote,
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
				})
			} else if !c.li.Dir && c.li.Dir {
				// We have a dir instead of a file on the server
				// Delete the local file and create a dir locally
				p.takeDecision(Decision{
					Flag:            DecisionDeleteLocalAndCreateDirLocal,
					RelativePath:    c.li.RelativePath,
					RemoteValidEtag: c.ri.Etag,
				})
				if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
					return false, err
				}
			} else {
				// We have the same type on both side
				if c.li.Etag != c.ri.Etag {
					p.takeDecision(Decision{
						Flag:            DecisionDownloadRemote,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
					})
				}
				// If it is a dir continue the inspection
				if c.li.Dir {
					if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
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
					p.takeDecision(Decision{
						Flag:            DecisionCreateDirLocal,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
					})
					if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
						return false, err
					}
				} else {
					p.takeDecision(Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
					})
				}
			} else if c.li.Dir && !c.li.Dir {
				// Dir locally and file remotely
				if c.li.Commited != CommitedAwaitingDeletion {
					deleted, err := p.checkChanges(c.li.RelativePath, false, false)
					if err != nil {
						return false, err
					}
					if deleted {
						p.takeDecision(Decision{
							Flag:            DecisionDownloadRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
						})
					} else {
						p.takeDecision(Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
						})
					}
				} else {
					p.takeDecision(Decision{
						Flag:            DecisionConflict,
						RelativePath:    c.li.RelativePath,
						RemoteValidEtag: c.ri.Etag,
					})
				}
			} else if c.li.Dir {
				// Both are directories
				if c.li.Commited == CommitedAwaitingDeletion {
					// Local dir wants to be deleted
					if _, err := p.checkChanges(c.li.RelativePath, false, true); err != nil {
						return false, err
					}
				} else {
					if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
						return false, err
					}
				}
			} else {
				// Both are files
				if c.li.Commited == CommitedAwaitingDeletion {
					if c.li.Etag == c.ri.Etag {
						// Etags are matching so we can delete
						p.takeDecision(Decision{
							Flag:            DecisionDeleteRemote,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
						})
						conDeleted += 1
					} else {
						p.takeDecision(Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
						})
					}
				} else {
					if c.li.Etag == c.ri.Etag {
						// Assuming that li.Etag is containing the old etag
						// So we upload
						p.takeDecision(Decision{
							Flag:            DecisionUploadLocal,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
						})
					} else {
						p.takeDecision(Decision{
							Flag:            DecisionConflict,
							RelativePath:    c.li.RelativePath,
							RemoteValidEtag: c.ri.Etag,
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
			p.takeDecision(Decision{
				Flag:            DecisionDeleteLocal,
				RelativePath:    relativePath,
				RemoteValidEtag: "",
			})
			return true, nil
		}
		if remoteDeleted {
			p.takeDecision(Decision{
				Flag:            DecisionDeleteRemote,
				RelativePath:    relativePath,
				RemoteValidEtag: "",
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
func (p *provider) CheckDecision(d Decision) (ok bool) {
	var ri *RemoteItem

	ris, err := p.remote.GetChildren(path.Base(d.RelativePath))
	if err != nil {
		return false
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
		return true
	case DecisionCreateDirRemote:
		if ri == nil {
			return true
		}
		return ri.Dir
	case DecisionDownloadRemote:
		// No impact as remote change was already there
		return true
	case DecisionDeleteLocal:
		// No impact as if remote changes this can be downloader later
		return true
	case DecisionConflict:
		// Conflict must be handled by the middleware
		return true
	case DecisionDeleteRemote:
		fallthrough
	case DecisionDeleteLocalAndCreateDirLocal:
		fallthrough
	case DecisionDeleteLocalAndDownloadRemote:
		fallthrough
	case DecisionUploadLocal:
		if ri == nil {
			return true
		}
		return ri.Etag == d.RemoteValidEtag
	}
	return false
}
