<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Publishing packages](#publishing-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Publishing packages

- Coordinate with committers to have a consensus that Jobserver is ready for a release.
- Finalize the next release number (e.g. 0.10.0) beforehand.
- Pull the latest code from `master` branch.
- Make sure that for next release `notes/` is already updated. You can use the following command to get a list of all commits since last release.
```
git log <last release commit hash>..master --format="- %s / (%an | %h)"
```
- Remove untracked files. You can use `git clean -fd`. Be careful, it will remove untracked files and directories.
- Make sure that `origin` is pointing to opensource github jobserver.
- Export bintray username/api key. To get API key, log into bintray > Edit profile > API Key.
```
export BINTRAY_USER=<username>
export BINTRAY_PASS=<API KEY HERE>
```
- In the root project, do `sbt release cross`
- The release process will ask you for next release number. Input the number.
- The release process will ask you to push some commits to `master` branch on `origin`. Type `y`. This creates commits like the following in master branch.
```
Setting version to 0.10.0
Setting version to 0.10.1-SNAPSHOT
```
- [Go releases page of Jobserver](https://github.com/spark-jobserver/spark-jobserver/releases). Click on the latest release and click on `Edit Tag` button. Put release notes and publish them.

Other:
- If you get an error like the following,
```
No tracking branch is set up. Either configure a remote tracking branch, or remove the pushChanges release part.
```
then you can fix it using
```
git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*
git config branch.master.remote origin
git config branch.master.merge refs/heads/master
```

To announce the release on [ls.implicit.ly](http://ls.implicit.ly/), use
[Herald](https://github.com/n8han/herald#install) after adding release notes in
the `notes/` dir.  Also regenerate the catalog with `lsWriteVersion` SBT task
and `lsync`, in project job-server.
