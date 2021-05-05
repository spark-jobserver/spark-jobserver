<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Publishing packages](#publishing-packages)
- [Releasing from a hotfix branch](#releasing-from-a-hotfix-branch)
- [Release process issues](#release-process-issues)
  - [No tracking branch is set up](#no-tracking-branch-is-set-up)
  - [Only one Scala version was published](#only-one-scala-version-was-published)
- [Known issues with test setup](#known-issues-with-test-setup)
  - [Failed migration or FlywayException in DAO tests](#failed-migration-or-flywayexception-in-dao-tests)
  - [Python tests errors](#python-tests-errors)

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
- Export your JFrog Platform username/password for https://sparkjobserver.jfrog.io:
```
export JFROG_USERNAME=<username>
export JFROG_PASSWORD=<API KEY HERE>
```
- In the root project, do `sbt 'release cross'`
- The release process will ask you for next release number. Input the number:
```
Release version [0.9.1] : 0.10.0
Next version [0.10.1-SNAPSHOT] :
```
- The release process will ask you to push some commits to `master` branch on `origin`. Type `y`. This creates commits like the following in master branch.
```
Setting version to 0.10.0
Setting version to 0.10.1-SNAPSHOT
```
- [Go releases page of Jobserver](https://github.com/spark-jobserver/spark-jobserver/releases). Click on the latest release and click on `Edit Tag` button. Put release notes and publish them.

### Releasing from a hotfix branch

Prerequisites:
- There is a hotfix branch (e.g. `jobserver-0.10.x`) with some fixes for the last release
- `origin` remote is pointing to Open Source github.com Jobserver

Execute
```
git push --set-upstream origin <<hotfix-branch-name>>
```
before starting the release process. The rest of the steps are identical to the release from the `master` branch.


### Release process issues

#### No tracking branch is set up

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

#### Only one Scala version was published

1. Reset your _local_ branch back to the last commit before the release.
    ```
    git reset --hard HEAD^^
    ```
2. Set Scala version to the Scala version, which was not published:
    ```
    export SCALA_VERSION=${NOT_PUBLISHED_VERSION}
    ```
3. Trigger release process for this Scala version:
    ```
    sbt release
    ```
4. Set release versions as in the main step (to exactly the same versions):
    ```
    Release version [0.9.1] : 0.10.0
    Next version [0.10.1-SNAPSHOT] :
    ```
5. SBT will complain, that the tag exists. Choose "keep":
    ```
    Tag [v0.11.0] exists! Overwrite, keep or abort or enter a new tag (o/k/a)? [a] k
    [warn] The current tag [v0.11.0] does not point to the commit for this release!
    ```
6. SBT will aks, if you want to push new commits, chose "no":
    ```
    Push changes to the remote repository (y/n)? [y] n
    [warn] Remember to push the changes yourself!
    ```
7. Check that all supported Scala versions are published (on JFrog).

### Known issues with test setup

Test issues are most likely connected to local setup as there is a Jenkins job running tests before
and after each PR is getting merged.
Usually problems arise due to misconfiguration of Spark paths or Python libraries.
It is a good practice to start debugging by running only problematic tests in an SBT console,
use `testOnly $specName` to run only specific test suite.

#### Failed migration or FlywayException in DAO tests

Make sure, that all old metadata files are deleted. Check for the current H2 DB files in the project
```
find . -name *h2.db
```
and delete them.
Also, clean up `/tmp` directory:
```
rm -r /tmp/spark-jobserver
```

#### Python tests errors

Our tests use both python2 and python3.
**Python 3 version should be below 3.8**

For Spark dependencies there are 2 ways:
- Set up `PYTHONPATH`: `export PYTHONPATH=$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip`
- Install dependencies with `pip`, e.g. `python -m pip install pyhocon pyspark==${SPARK_VERSION}`
