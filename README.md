# Fenritec Fast Sync

Fenritec Fast Sync is a library to help syncing two directories for cloud sync providers.

## Concept

We assume that a commit status on the local system for a relative path is stored. It could be :
- Commited : The File / Dir has beed pushed the remote server
- NotCommited : The File / Dir has not been pushed to the serve
- AwaitingDeletion : The File / Dir has been deleted locally but has not been commited on the remote server

The Etag helps to ensure that file are commited on the server only on need.
We assume that the ETAG has always the value of the last commited file.

Thus if you have a new file, the ETAG must be an empty string.

## Warning

If you want to delete folder /a with all of its sub-folders from the local fs, you need to have AwaitingDeletion for the folder and the sub-items.
Otherwise the lib will consider that /a wants to be deleted locally and that new files are on a remotely

