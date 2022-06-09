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
After your first contribution was done. You cloud choose other issues you interested in to continuing contributing, 

### Submit an Issue
Submit an issue to Vanus is a pretty important contributing. Each issue whatever which about bug/feature request/improvement/
blog/looking for helping/confusing is ESSENTIAL for making Vanus greater.

You could click `new` button in (issues sub-page)[https://github.com/linkall-labs/vanus/issues] of Vanus project to 
submit an issue for us.

## PRs

### Format

#### Title
This repo's PR title based on [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)

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

#### Descriptions


### Testings

### Sign the CLA
You need to sign the CLA(Contributor License Agreement) of Linkall for contributing, click here know how.

### Automatic Checking

### Code Review

### Discussion

### Merging