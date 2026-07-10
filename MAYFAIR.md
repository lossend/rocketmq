# Mayfair RocketMQ 5.5.0 Patch Specification

## Overview

This repository is Mayfair's internal fork of Apache RocketMQ 5.5.0, maintained to backport upstream bug fixes that are not yet included in the official 5.5.0 release.

## Branch Strategy

| Branch | Purpose |
|---|---|
| `mayfair/rocketmq-5.5.0` | Company trunk — accumulates all patches on top of 5.5.0 |
| `fix/<issue>-<description>` | Per-fix branch based on the 5.5.0 release commit (`98799686a`) |

## Docker Image Tags

| Tag | Purpose |
|---|---|
| `mayfair/rocketmq:5.5.0` | Always points to the latest patched build |
| `mayfair/rocketmq:5.5.0-patch1` | Versioned snapshot after 1st patch merge |
| `mayfair/rocketmq:5.5.0-patch2` | Versioned snapshot after 2nd patch merge |

## Workflow

### Apply a New Fix

```bash
# 1. Create a fix branch from the 5.5.0 release
git checkout -b fix/<issue>-<description> 98799686a

# 2. Cherry-pick the upstream fix commit
git cherry-pick <upstream-commit>

# 3. Merge into company trunk
git checkout mayfair/rocketmq-5.5.0
git merge fix/<issue>-<description>

# 4. Build and tag Docker image
docker build -t mayfair/rocketmq:5.5.0-patch<N> .
docker build -t mayfair/rocketmq:5.5.0 .
```

## Patch History

| Patch | Branch | Upstream PR | Issue | Description |
|---|---|---|---|---|
| patch1 | `fix/10284-consumer-offset` | [#10287](https://github.com/apache/rocketmq/pull/10287) | #10284 | Fix consumerOffset deserialization error when upgrading from 5.4.x to 5.5.0 |
