<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Contribution guidelines](#contribution-guidelines)
  - [<a name="pullrequest"></a> Pull Requests Guidelines](#a-namepullrequesta-pull-requests-guidelines)
  - [<a name="commit"></a> Git Commit Guidelines](#a-namecommita-git-commit-guidelines)
    - [Commit Message Format](#commit-message-format)
      - [Type](#type)
      - [Scope](#scope)
      - [Subject](#subject)
      - [Body](#body)
      - [Footer](#footer)
      - [Examples](#examples)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Contribution guidelines

- [Pull Requests](#pullrequest)
- [Commit Message Guidelines](#commit)

## <a name="pullrequest"></a> Pull Requests Guidelines
Pull requests are the only means by which you can contribute to this project, please follow the following steps when submitting pull requests :

1. Create your pull request. (PR name must match [commit name](#commit))
2. Fill the pull request template with all of the required elements
3. CI tests kick in and report back any issues, you should fix these issues before continuing.
4. The reviewer(s) uses Github review system so you will know if the reviewer requested any changes, approved the Pull Request or simply added comments.
5. If any changes are requested please fix them and then once you are ready ask for a new review

## <a name="commit"></a> Git Commit Guidelines 

### Commit Message Format
A commit message is made up of a **header**, a **body** and a **footer**.  The header has a special
format that includes a **type**, a **scope** and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

* The **header** and **scope** are mandatory
* Lines shouldn't exceed 100 characters

#### Type
Must be one of the following:

* **feat**: A new feature
* **fix**: A bug fix
* **refactor**: Refactoring change
* **docs**: Documentation only changes
* **style**: Style changes (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests
* **misc**: Changes to the build process or auxiliary tools and libraries

#### Scope
The scope sets the scope of the commit, for example `webapi`, `akka-app`, `doa`, etc...

#### Subject
Brief commit description

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize first letter
* no dot (.) at the end

#### Body
Extended commit description and just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes".
The body should include the motivation for the change and contrast this with previous behaviour if any.

#### Footer

* any information about **Breaking Changes**

**Breaking Changes** should start with the word `BREAKING CHANGES:` followed by a new line and a list of breaking changes.

#### Examples

```
feat(akka-app): change message serialization format

* Add new serialization format
* Add serialization tests
* Document the changes

BEARKING CHANGES :
* Not compatible with 0.7.0
```

---

```
doc(webapi): add new routes documentation

* Document all new routes
* Add exmaple snippets
* Remove obselete links

```

---

```
fix(webapi): fix webapi timeout
```

---

```
misc(ci): add new CI tests
```
