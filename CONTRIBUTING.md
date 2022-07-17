# How about contributing to Vanus

Thanks for your focus on how to contribute to Vanus, we expect meet to you in Vanus Community. Any contributions including but 
not only like submit issue/codebase/docs/tests/blogs are welcomed. Contributions to Vanus project are expected to adhere our
[Code of Conduct](CODE-OF-CONDUCT.md).

This document outlines some conventions about development workflow, commit message formatting, contact points and other
resources to make it easier to get your contribution accepted.

<!-- TOC -->
- [How about contributing to Vanus](#how-about-contributing-to-vanus)
- [Before you get started](#before-you-get-started)
  - [Sign the CLA](#sign-the-cla)
  - [Setting up your development environment(optional)](#setting-up-your-development-environmentoptional)
- [Issues](#issues)
  - [Choosing](#choosing)
    - [First Contribution](#first-contribution)
    - [Further Contributions](#further-contributions)
  - [Submit an Issue](#submit-an-issue)
- [Pull Requests](#pull-requests)
  - [Style](#style)
  - [Descriptions](#descriptions)
  - [Testings](#testings)
  - [CI Checking](#ci-checking)
  - [Code Review](#code-review)
- [Reward](#reward)
<!-- /TOC -->

## Before you get started

### Sign the CLA
Click [here](https://cla-assistant.io/linkall-labs/) to sign the CLA, and click the `Sign in with GitHub to agree` button to sign.

What is [CLA](https://en.wikipedia.org/wiki/Contributor_License_Agreement).

### Setting up your development environment(optional)
If you want to contribute to the codebase of Vanus, it's better to set up a development environment in local first.

1. Install Go version 1.17 or above. Refer to [How to Write Go Code](https://go.dev/doc/code) for more information.
2. Install [minikube](https://minikube.sigs.k8s.io/docs/start/)(Users from China mainland may have network issues that need to be addressed for using minikube). 
Vanus is a k8s based project.
3. Learning [QuickStart](https://github.com/linkall-labs/docs/blob/main/vanus/quick-start.md) how to deploy the Vanus to k8s. Deploying a specified component to minikube for testing. 
Click [here](#) for details.

## Issues
After set up done, the next step is to choose an issue to address. Issues are all problems of Vanus that need to be addressed.

### Choosing
You can find all issues in [vanus-issues](https://github.com/linkall-labs/vanus/issues) and choose one which isn't
assigned to someone.

#### First Contribution
You can start by finding existing issues with the 
[good-first-issue](https://github.com/linkall-labs/vanus/issues?q=is%3Aopen+is%3Aissue+label%3Agood-first-issue) label. 
These issues are well suited for a new contributor.

#### Further Contributions
After your first contribution was done, you cloud choose other issues you are interested in to continue contributing. You also
could create a PR directly to Vanus for fixing or improving.

### Submit an Issue
Submitting an issue to Vanus is a pretty important contribution. Each issue whatever which about bug/feature request/improvement/
blog/looking for help/confusing is ESSENTIAL for making Vanus greater.

You could click the `New issue` button in (issues sub-page)[https://github.com/linkall-labs/vanus/issues] of Vanus project to 
submit an issue for us.

## Pull Requests
After your work is done, you can create a `Pull Request` by clicking the `New pull request` button in
[pull requests](https://github.com/linkall-labs/vanus/pulls) page to submit your contribution to Vanus.
More about `Pull Request` you can find [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests).

There are some constraints that in order to save reviewer's time you need to pay attention to.

### Style
We strictly check PR title's style. The PR's title follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)

- feat: A new feature
- fix: A bug fix
- docs: Documentation only changes
- style: Changes that do not affect the meaning of the code (white space, formatting, missing semi-colons, etc)
- refactor: A code change that neither fixes a bug nor adds a feature
- perf: A code change that improves performance
- test: Adding missing tests or correcting existing tests
- build: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- ci: Changes to CI configuration files and scripts
- chore: Other changes that don't modify src or test files
- revert: Reverts a previous commit

Examples:

```text
test: fix unstable ut of wait.go 
feat: waiting for ack of append from raft
```

### Descriptions
We provide the template for each PR and expect you could explain your PR for what/why/how in detail. 
If the PR wants to address one issue, please link to it.

### Testings
We hope you can test well your PR before starting the Code Review. If the PR is about a new feature, refactor or bugfix, the related
Unit tests will be required.

### CI Checking
There are some automated tasks to ensure each PR's reach high quality in Vanus. These tasks are:
- License checker: check if a new source code file includes the 'Apache License 2.0' Header. The auto-generated file isn't required.
- golang-lint: run lint checker to analyze code to find the hidden risk in code.
- CLA checker: checkout if the contributor signed the Contributor License Agreement.
- codecov: run all unit tests and generate unit test report. If any case failed or the coverage number is down, the task will be failed.
- e2e testing: run the e2e testing. Any case that failed will cause the task to failed.

### Code Review
The Code Review can be started until all checking were passed. Each PR will automatically assign reviewers. After getting 
at least one `Approve` from reviewers, the PR can be merged.

## Reward
Once your PR has been merged, you become a Vanus Contributor. Thanks for your contribution! please fill the [form](#) to get you reward