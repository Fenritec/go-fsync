package fsync

import (
	"context"
	"encoding/json"
)

type (
	Provider interface {
		CheckChanges(ctx context.Context, rPath string) error
		DoInitialSync(ctx context.Context) error
		CheckDecision(ctx context.Context, d Decision) (err error, ok bool)
	}

	provider struct {
		local  LocalFS
		remote RemoteFS

		takeDecision DecisionCallback
		localChange  chan (LocalItem)
		remoteChange chan (RemoteItem)

		remoteFSDeleteNonEmptyFolder bool
		localFSDeleteNonEmptyFolder  bool
	}

	Options struct {
		// RemoteFSDeleteNonEmptyFolder allows to delete the folder instead of all items + folder to avoid huge calls
		RemoteFSDeleteNonEmptyFolder bool
		// LocalFSDeleteNonEmptyFolder allows to delete the folder instead of all items + folder to avoid huge calls
		LocalFSDeleteNonEmptyFolder bool
	}

	LocalFS interface {
		GetChildren(itemPath string) (LocalItems, error)
	}

	LocalItem struct {
		RelativePath string
		Dir          bool
		// Etag is the Etag of the last commited item (empty for folders)
		Etag     string
		Commited CommitedFlag
	}

	CommitedFlag int

	LocalItems []LocalItem

	RemoteFS interface {
		GetChildren(itemPath string) (RemoteItems, error)
	}

	RemoteItem struct {
		RelativePath string
		Dir          bool
		Etag         string
	}

	RemoteItems []RemoteItem

	Decision struct {
		RelativePath    string
		Flag            DecisionFlag
		RemoteValidEtag string
		RemoteIsDir     bool
		Why             DecisionWhy
	}

	DecisionWhy struct {
		LocalItemPresent  bool   `json:"local_item_present"`
		LocalItemDir      bool   `json:"local_item_dir"`
		LocalItemEtag     string `json:"local_item_etag"`
		LocalItemCommited string `json:"local_item_commited"`

		RemoteItemPresent bool   `json:"remote_item_present"`
		RemoteItemDir     bool   `json:"remote_item_dir"`
		RemoteItemEtag    string `json:"remote_item_etag"`
	}

	DecisionCallback func(context.Context, Decision) error

	DecisionFlag int

	Conflict struct {
		li LocalItem
		ri RemoteItem
	}

	Conflicts []Conflict
)

const (
	DecisionUploadLocal = DecisionFlag(iota)
	DecisionCreateDirLocal
	DecisionCreateDirRemote
	DecisionDownloadRemote
	DecisionDeleteLocal
	DecisionDeleteRemote
	DecisionConflict
	DecisionDeleteMetadata
	DecisionDeleteLocalAndCreateDirLocal
	DecisionDeleteLocalAndDownloadRemote
)

const (
	CommitedYes = CommitedFlag(iota)
	CommitedNo
	CommitedAwaitingRemoteDeletion
)

func (p *provider) LocalChange(item LocalItem) {
	p.localChange <- item
}

func (p *provider) RemoteChange(item RemoteItem) {
	p.remoteChange <- <-p.remoteChange
}

func (f CommitedFlag) ToString() string {
	switch f {
	case CommitedYes:
		return "CommitedYes"
	case CommitedNo:
		return "CommitedNo"
	case CommitedAwaitingRemoteDeletion:
		return "CommitedAwaitingDeletion"
	}
	return ""
}

func (d DecisionFlag) ToString() string {
	switch d {
	case DecisionUploadLocal:
		return "DecisionUploadLocal"
	case DecisionCreateDirLocal:
		return "DecisionCreateDirLocal"
	case DecisionCreateDirRemote:
		return "DecisionCreateDirRemote"
	case DecisionDownloadRemote:
		return "DecisionDownloadRemote"
	case DecisionDeleteLocal:
		return "DecisionDeleteLocal"
	case DecisionDeleteRemote:
		return "DecisionDeleteRemote"
	case DecisionConflict:
		return "DecisionConflict"
	case DecisionDeleteLocalAndCreateDirLocal:
		return "DecisionDeleteLocalAndCreateDirLocal"
	case DecisionDeleteLocalAndDownloadRemote:
		return "DecisionDeleteLocalAndDownloadRemote"
	}
	return ""
}

func newDecisionWhy(li *LocalItem, ri *RemoteItem) DecisionWhy {
	d := DecisionWhy{}

	if li != nil {
		d.LocalItemCommited = li.Commited.ToString()
		d.LocalItemDir = li.Dir
		d.LocalItemEtag = li.Etag
		d.LocalItemPresent = true
	}

	if ri != nil {
		d.RemoteItemDir = ri.Dir
		d.RemoteItemEtag = ri.Etag
		d.RemoteItemPresent = true
	}

	return d
}

func (d DecisionWhy) ToJSONString() string {
	data, _ := json.Marshal(d)
	return string(data)
}
