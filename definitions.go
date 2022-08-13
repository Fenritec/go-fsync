package fsync

import "context"

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
