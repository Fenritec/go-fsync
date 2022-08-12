package fsync

import "strings"

func Less(i, j Decision) bool {
	jChildOfI := false
	if strings.HasPrefix(j.RelativePath, i.RelativePath) {
		jChildOfI = true
	} else if !strings.HasPrefix(i.RelativePath, j.RelativePath) {
		return strings.Compare(i.RelativePath, j.RelativePath) <= 0
	}

	var d Decision
	if jChildOfI {
		d = i
	} else {
		d = j
	}

	var parentAfter bool
	switch d.Flag {
	case DecisionConflict:
		fallthrough
	case DecisionCreateDirLocal:
		fallthrough
	case DecisionCreateDirRemote:
		fallthrough
	case DecisionUploadLocal:
		fallthrough
	case DecisionDownloadRemote:
		fallthrough
	case DecisionDeleteLocalAndDownloadRemote:
		parentAfter = false
	case DecisionDeleteLocal:
		fallthrough
	case DecisionDeleteRemote:
		fallthrough
	case DecisionDeleteLocalAndCreateDirLocal:
		parentAfter = true
	}

	if jChildOfI {
		if parentAfter {
			return false
		}
		return true
	}

	if parentAfter {
		return true
	}
	return false
}
