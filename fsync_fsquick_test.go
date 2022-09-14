package fsync_test

import (
	"testing"

	"github.com/fenritec/go-fsync"
)

func testScenarioQuick(t *testing.T, lst fsync.LocalItems, rst fsync.RemoteItems, expectedDecisions []fsync.Decision) {
	testScenarioWithOptions(t, lst, rst, expectedDecisions, &fsync.Options{
		RemoteFSDeleteNonEmptyFolder: true,
		LocalFSDeleteNonEmptyFolder:  true,
	})
}

func TestProviderQuick(t *testing.T) {
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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
			{RelativePath: "/c", Flag: fsync.DecisionDeleteLocal},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Files and folder deleted on local", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: true, Etag: "", Commited: fsync.CommitedAwaitingRemoteDeletion},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingRemoteDeletion},
			{RelativePath: "/a/d", Dir: false, Etag: "v1", Commited: fsync.CommitedYes},
			{RelativePath: "/b", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingRemoteDeletion},
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
			{RelativePath: "/c", Flag: fsync.DecisionDeleteRemote},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("File and folder conflict", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Etag: "", Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: false, Etag: "v1"},
			{RelativePath: "/c", Dir: true, Etag: ""},
			{RelativePath: "/c/d", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionConflict},
			{RelativePath: "/c", Flag: fsync.DecisionConflict},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Folder deletion local new file remote", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: false, Etag: "v1", Commited: fsync.CommitedAwaitingRemoteDeletion},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true},
			{RelativePath: "/a/b", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b", Flag: fsync.DecisionDownloadRemote},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b", Flag: fsync.DecisionUploadLocal},
			{RelativePath: "/a/c", Flag: fsync.DecisionConflict},
			{RelativePath: "/a/d", Flag: fsync.DecisionDownloadRemote},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Deleted dir on local but new files in dir on remote", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedAwaitingRemoteDeletion},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: true, Etag: ""},
			{RelativePath: "/a/b/c", Dir: false, Etag: "v1"},
			{RelativePath: "/a/d", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b", Flag: fsync.DecisionCreateDirLocal},
			{RelativePath: "/a/b/c", Flag: fsync.DecisionDownloadRemote},
			{RelativePath: "/a/d", Flag: fsync.DecisionDownloadRemote},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
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

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("New files on remote", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: false, Commited: fsync.CommitedYes, Etag: "v1"},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
			{RelativePath: "/a/b", Dir: false, Etag: "v2"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionDownloadRemote},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Local awaiting deletion but no remote file", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Commited: fsync.CommitedYes},
			{RelativePath: "/a/b", Dir: false, Commited: fsync.CommitedAwaitingRemoteDeletion, Etag: "v1"},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: true, Etag: ""},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a/b", Flag: fsync.DecisionDeleteLocal},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})

	t.Run("Out of sync merge with diff on both side", func(t *testing.T) {
		localStatus := fsync.LocalItems{
			{RelativePath: "/a", Dir: true, Etag: "", Commited: fsync.CommitedNo},
			{RelativePath: "/a/b", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
			{RelativePath: "/c", Dir: false, Etag: "v1", Commited: fsync.CommitedNo},
		}

		remoteStatus := fsync.RemoteItems{
			{RelativePath: "/a", Dir: false, Etag: "v1"},
			{RelativePath: "/c", Dir: true, Etag: ""},
			{RelativePath: "/c/d", Dir: false, Etag: "v1"},
		}

		expectedDecisions := []fsync.Decision{
			{RelativePath: "/a", Flag: fsync.DecisionConflict},
			{RelativePath: "/c", Flag: fsync.DecisionConflict},
		}

		testScenarioQuick(t, localStatus, remoteStatus, expectedDecisions)
	})
}
