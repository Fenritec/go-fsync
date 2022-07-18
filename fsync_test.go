package fsync_test

import (
	"path"
	"testing"
	"time"

	"github.com/fenritec/go-fsync"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

type (
	localFS struct {
		status fsync.LocalItems
	}

	remoteFS struct {
		status fsync.RemoteItems
	}
)

func (l *localFS) GetChildren(relativePath string) (fsync.LocalItems, error) {
	ret := fsync.LocalItems{}
	for _, item := range l.status {
		if path.Dir(item.RelativePath) == relativePath {
			ret = append(ret, item)
		}
	}
	return ret, nil
}

func (l *remoteFS) GetChildren(relativePath string) (fsync.RemoteItems, error) {
	ret := fsync.RemoteItems{}
	for _, item := range l.status {
		if path.Dir(item.RelativePath) == relativePath {
			ret = append(ret, item)
		}
	}
	return ret, nil
}

func IsDecisionPresent(d fsync.Decision, in []fsync.Decision) bool {
	for _, d2 := range in {
		if d.Flag == d2.Flag && d.RelativePath == d2.RelativePath {
			return true
		}
	}
	return false
}

func testScenario(t *testing.T, lst fsync.LocalItems, rst fsync.RemoteItems, expectedDecisions []fsync.Decision) {
	lFS := localFS{
		status: lst,
	}

	rFS := remoteFS{
		status: rst,
	}

	decisions := []fsync.Decision{}
	cb := fsync.DecisionCallback(func(d fsync.Decision) {
		decisions = append(decisions, d)
	})

	p := fsync.NewProvider(&lFS, &rFS, cb)
	err := p.DoInitialSync()
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond)
	t.Logf("%d decisions made", len(decisions))
	for _, d := range decisions {
		t.Logf("Decision flag %s, relativePath: %s", d.Flag.ToString(), d.RelativePath)
	}

	assert.Equal(t, len(expectedDecisions), len(decisions))
	for _, d := range expectedDecisions {
		assert.Equal(t, true, IsDecisionPresent(d, decisions))
	}
}

func TestProvider(t *testing.T) {
	t.Run("Empty local dir intial merge", func(t *testing.T) {
		localStatus := fsync.LocalItems{}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: true, Etag: ""},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
			{RelativePath: "/b", Dir: true, Etag: ""},
			{RelativePath: "/c", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionDownloadRemote},
			{RelativePath: "/a/d", Flag: fsync.DecisionDownloadRemote},
			{RelativePath: "/b", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/c", Flag: fsync.DecisionDownloadRemote},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Empty remote dir intial merge", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Etag: "", Commited: fsync.CommitedNo},
			{RelativePath: "/a/b", Dir: true, Etag: "", Commited: fsync.CommitedNo},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
			{RelativePath: "/a/d", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
			{RelativePath: "/b", Dir: true, Etag: "", Commited: fsync.CommitedNo},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirRemote},
			{RelativePath: "/a/b", Flag: fsync.DecisionCreateDirRemote},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionUploadLocal},
			{RelativePath: "/a/d", Flag: fsync.DecisionUploadLocal},
			{RelativePath: "/b", Flag: fsync.DecisionCreateDirRemote},
			{RelativePath: "/c", Flag: fsync.DecisionUploadLocal},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("In Sync nothing to do", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/a/d", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: true, Etag: ""},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
			{RelativePath: "/b", Dir: true, Etag: ""},
			{RelativePath: "/c", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Files and folder deleted on server", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/a/d", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
			{RelativePath: "/b", Dir: true, Etag: ""},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionDeleteLocal},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionDeleteLocal},
			{RelativePath: "/c", Flag: fsync.DecisionDeleteLocal},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Files and folder deleted on local", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: true, Etag: "", Commited: fsync.CommitedAwaitingDeletion},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingDeletion},
			{RelativePath: "/a/d", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingDeletion},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: true, Etag: ""},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
			{RelativePath: "/b", Dir: true, Etag: ""},
			{RelativePath: "/c", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionDeleteRemote},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionDeleteRemote},
			{RelativePath: "/c", Flag: fsync.DecisionDeleteRemote},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("File conflict", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: false, Etag: "v2", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionConflict},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Folder deletion local new file remote", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingDeletion},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true},
			{RelativePath: "/a/b", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b", Flag: fsync.DecisionDownloadRemote},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Folder deletion remote new file local", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedNo},
			{RelativePath: "/a/b", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirRemote},
			{RelativePath: "/a/b", Flag: fsync.DecisionUploadLocal},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Creating same dir on both side with partial conflict", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedNo},
			{RelativePath: "/a/b", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
			{RelativePath: "/a/c", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/c", Dir: false, Etag: "v2"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionUploadLocal},
			{RelativePath: "/a/c", Flag: fsync.DecisionConflict},
			{RelativePath: "/a/d", Flag: fsync.DecisionDownloadRemote},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Deleted dir on local but new files in dir on remote", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedAwaitingDeletion},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: true, Etag: ""},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionDownloadRemote},
			{RelativePath: "/a/d", Flag: fsync.DecisionDownloadRemote},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("New files on local", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: false, Commited: fsync.CommitedNo, Etag: "v1"},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionUploadLocal},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Local awaiting deletion but no remote file", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: false, Commited: fsync.CommitedAwaitingDeletion, Etag: "v1"},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionDeleteLocal},
		}

		testScenario(t, localStatus, remoteStatus, expectedDecisions)
	})
}
