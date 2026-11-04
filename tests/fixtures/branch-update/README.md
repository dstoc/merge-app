# Branch-update conflict payloads

Captured on 2026-11-04 from GitHub GraphQL `updatePullRequestBranch` on
[disposable draft PR 20](https://github.com/dstoc/merge-app/pull/20), for
merge-app/21. The test used isolated base/head branches, no auto-merge, and the
runner's user credential. It did not validate App installation permissions.
The query requested `pullRequest { id headRefOid baseRefOid autoMergeRequest {
mergeMethod } }` and supplied `expectedHeadOid` and an explicit update method.

`rebase-conflict.json` came from this history:

```text
root: conflict.txt = original
  ├─ feature: intermediate → final
  └─ base: final, plus base.txt
```

Replaying the intermediate feature commit conflicts. Updating with `MERGE` on
the same tips succeeded. The mutation response still had the old head; a later
read showed a merge commit and an unchanged target tip.

After that merge, each branch changed `conflict.txt` to a different value.
REBASE returned `rebase-conflict-class.json`; MERGE returned
`merge-conflict.json`. Both left the working head unchanged. These are the only
content-conflict messages accepted by the classifier, with the captured type,
path, and null mutation result. Unknown messages, mixed errors, partial success,
and a message captured for the other method remain failures.

The target advance also exposed stale `baseRefOid` (and REST `base.sha`).
`baseRef.target.oid` and `git ls-remote` reported the actual new target tip.
Queue reads use the live ref target. The decoder test deliberately supplies
different cached and current OIDs to protect this distinction.

GitHub can change its error payloads. Capture new evidence and update tests
before extending this allowlist. Do not classify the word `conflict` or an HTTP
status alone as a content conflict.
