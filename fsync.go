package fsync

func NewProvider(l LocalFS, r RemoteFS, d DecisionCallback) Provider {
	p := &provider{
		local:  l,
		remote: r,

		out:          d,
		localChange:  make(chan LocalItem, 100),
		remoteChange: make(chan RemoteItem, 100),
	}

	return p
}

func (p *provider) DoInitialSync() error {
	return p.CheckChanges("/")
}

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
				p.out(Decision{
					Flag:         DecisionDeleteLocal,
					RelativePath: e.RelativePath,
				})
				expDeleted += 1
			}
		} else {
			if e.Dir {
				if e.Commited == CommitedAwaitingDeletion {
					p.out(Decision{
						Flag:         DecisionDeleteLocal,
						RelativePath: e.RelativePath,
					})
				} else {
					p.out(Decision{
						Flag:         DecisionCreateDirRemote,
						RelativePath: e.RelativePath,
					})
					if _, err := p.checkChanges(e.RelativePath, false, false); err != nil {
						return false, err
					}
				}
			} else {
				p.out(Decision{
					Flag:         DecisionUploadLocal,
					RelativePath: e.RelativePath,
				})
			}
		}
	}

	// Importing
	for _, i := range imp {
		if i.Dir {
			p.out(Decision{
				Flag:         DecisionCreateDirLocal,
				RelativePath: i.RelativePath,
			})
			if _, err := p.checkChanges(i.RelativePath, false, false); err != nil {
				return false, err
			}
		} else {
			p.out(Decision{
				Flag:         DecisionDownloadRemote,
				RelativePath: i.RelativePath,
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
				p.out(Decision{
					Flag:         DecisionDeleteLocal,
					RelativePath: c.li.RelativePath,
				})
				p.out(Decision{
					Flag:         DecisionDownloadRemote,
					RelativePath: c.li.RelativePath,
				})
			} else if !c.li.Dir && c.li.Dir {
				// We have a dir instead of a file on the server
				// Delete the local file and create a dir locally
				p.out(Decision{
					Flag:         DecisionDeleteLocal,
					RelativePath: c.li.RelativePath,
				})
				p.out(Decision{
					Flag:         DecisionCreateDirLocal,
					RelativePath: c.li.RelativePath,
				})
				if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
					return false, err
				}
			} else {
				// We have the same type on both side
				if c.li.Etag != c.ri.Etag {
					p.out(Decision{
						Flag:         DecisionDownloadRemote,
						RelativePath: c.li.RelativePath,
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
					p.out(Decision{
						Flag:         DecisionCreateDirLocal,
						RelativePath: c.li.RelativePath,
					})
					if _, err := p.checkChanges(c.li.RelativePath, false, false); err != nil {
						return false, err
					}
				} else {
					p.out(Decision{
						Flag:         DecisionConflict,
						RelativePath: c.li.RelativePath,
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
						p.out(Decision{
							Flag:         DecisionDownloadRemote,
							RelativePath: c.li.RelativePath,
						})
					} else {
						p.out(Decision{
							Flag:         DecisionConflict,
							RelativePath: c.li.RelativePath,
						})
					}
				} else {
					p.out(Decision{
						Flag:         DecisionConflict,
						RelativePath: c.li.RelativePath,
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
						p.out(Decision{
							Flag:         DecisionDeleteRemote,
							RelativePath: c.li.RelativePath,
						})
						conDeleted += 1
					} else {
						p.out(Decision{
							Flag:         DecisionConflict,
							RelativePath: c.li.RelativePath,
						})
					}
				} else {
					if c.li.Etag == c.ri.Etag {
						// Assuming that li.Etag is containing the old etag
						// So we upload
						p.out(Decision{
							Flag:         DecisionUploadLocal,
							RelativePath: c.li.RelativePath,
						})
					} else {
						p.out(Decision{
							Flag:         DecisionConflict,
							RelativePath: c.li.RelativePath,
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
			p.out(Decision{
				Flag:         DecisionDeleteLocal,
				RelativePath: relativePath,
			})
			return true, nil
		}
		if remoteDeleted {
			p.out(Decision{
				Flag:         DecisionDeleteRemote,
				RelativePath: relativePath,
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
