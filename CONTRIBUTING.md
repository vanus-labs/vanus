# How about contributing to Vanus

Thanks for your focus on how contribute to Vanus, we expect meet you in Vanus Community. Any contributions including but 
not only like submit issue/codebase/docs/tests/blogs are welcomed. Contributions to Vanus project are expected to adhere our
[Code of Conduct](CODE-OF-CONDUCT.md).

This document outlines some conventions about development workflow, commit message formatting, contact points and other
resources to make it easier to get your contribution accepted.

<!-- TOC -->
- [How about contributing to Vanus](#how-about-contributing-to-vanus)
<!-- /TOC -->

## Before you get started

### Sign the CLA
Click the Sign in with GitHub to agree button to sign the CLA. See an example [here](#).

What is [CLA](#)?

### Setting up your development environment(optional)
If you want to contribute to codebase of Vanus, it's better to set up development environment in local first.

1. Install Go version 1.17 or above. Refer to [How to Write Go Code](https://go.dev/doc/code) for more information.
2. Install [minikube](#)(Users from China mainland may have network issue need to be addressed for using minikube). 
Vanus is a k8s based project.
3. Learning [QuickStart](#) how deploy the Vanus to k8s. Deploying a specified component to minikube for testing. 
Click [here](#) for details.

## Issues
After all are set done, next step is choose an issue to addressed. Issues are all problems of Vanus need to be addressed.

### Choosing
You can find all issues in [vanus-issues](https://github.com/linkall-labs/vanus/issues) and choose one which isn't
assigned to someone.

#### First Contribution
You can start by finding an existing issues with the 
[good-first-issue](https://github.com/linkall-labs/vanus/issues?q=is%3Aopen+is%3Aissue+label%3Agood-first-issue) label. 
These issues are well suited for a new contributor.

#### Further Contributions
After your first contribution was done, you cloud choose other issues you interested in to continuing contributing. You also
could create a PR directly to Vanus for fixing or improving.

### Submit an Issue
Submit an issue to Vanus is a pretty important contributing. Each issue whatever which about bug/feature request/improvement/
blog/looking for helping/confusing is ESSENTIAL for making Vanus greater.

You could click `New issue` button in (issues sub-page)[https://github.com/linkall-labs/vanus/issues] of Vanus project to 
submit an issue for us.

## Pull Requests
After your work done, you can create a `Pull Request` by click `New pull request` button in
[pull requests](https://github.com/linkall-labs/vanus/pulls) pages to submit your contribution to Vanus.
More about `Pull Request` you can find [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests).

There are some constraints that in order to save review's time you need pay attention to.

### Style
We strictly check PR's format. A PR consist of two part: Title and Description.

#### Title
The PR's title follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)

- feat: A new feature
- fix: A bug fix
- docs: Documentation only changes
- style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- refactor: A code change that neither fixes a bug nor adds a feature
- perf: A code change that improves performance
- test: Adding missing tests or correcting existing tests
- build: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- ci: Changes to RisingWave CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
- chore: Other changes that don't modify src or test files
- revert: Reverts a previous commit

Examples:

```text
test: fix unstable ut of wait.go 
feat: waiting for ack of append from raft
```

#### Descriptions
We provide the template for each PR and expect you could explain your PR for what/why/how in detail. 
If the PR want to address one issue, please link to.

### Testings
We hope you can test well your PR before starting the Code Review. If the PR is about a new feature or bugfix, the related
Unit tests will be required.

### Checking
There are many automated task to ensure each PR's reach a high quality in Vanus. These tasks are:
- License checker: check if a new source code file include 'Apache License 2.0' Header. The auto-generated file isn't required.
- golang-lint: run lint checker to analyse code to find the hidden risk in code.
- CLA checker: checkout if the contributor signed the Contributor License Agreement.
- codecov: run all unit tests and generate unit test report. If any case failed or the coverage number down, the task will be failed.
- e2e testing: run the e2e testing. Any case failed will cause the task failed.

### Code Review
The Code Review can be started until all checking were passed. Each PR will automatically assign reviewers. After getting 
at least one `Approve` from reviewers, the PR can be merged.

## Rewards
TODO