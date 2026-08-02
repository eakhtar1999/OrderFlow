# Deep dive: the git + GitHub workflow behind this repo

Every command and every log excerpt below is REAL — pulled from this
project's own history, not invented for illustration. Where something
genuinely wasn't used in this repo yet (cherry-pick, tags), that's
called out explicitly rather than faked. Read this after skimming the
root README's own "history reconstruction" note in Step 1's commit
message — that note explains WHY the first 8 commits exist the way they
do; this doc explains HOW everything from Step 10 onward actually got
built, commit by commit, on real feature branches, merged through real
pull requests.

## The cast of components

| Component | What it actually is |
|---|---|
| **Working tree** | The files on disk, right now — what your editor sees. |
| **Staging area (the "index")** | A snapshot-in-progress. `git add` copies a file's CURRENT working-tree state into it; `git commit` seals whatever's staged into a permanent commit. Nothing in the working tree is ever committed directly — it's always working tree → staging → commit. |
| **Commit** | An immutable snapshot of the ENTIRE tracked tree at that point, not a diff (git computes diffs on demand for display; it doesn't store them). Identified by a SHA-1 hash of its content + metadata + parent(s). |
| **Branch** | A movable pointer to one commit — nothing more. `git branch -a` at any point in this repo's life just lists which pointers currently exist. |
| **`HEAD`** | A pointer to "whichever branch (or commit) you currently have checked out." Moving `HEAD` is what `git checkout`/`git switch` do. |
| **Remote** | A named reference to another repository's URL — `origin` in this repo, pointing at `https://github.com/eakhtar1999/OrderFlow.git`. `origin/main` is a LOCAL bookmark for "where main was, as of the last fetch" — it does not update itself; only `git fetch`/`git pull`/`git push` move it. |
| **Merge commit** | A commit with TWO parents instead of one — the record that two divergent histories were reconciled. This repo's `6747e09` is a real one (see Stage D). |

---

## Stage A — the repo's actual origin story (Steps 1–9)

This project was built in one continuous working session, not committed
incrementally as it went — then reconstructed afterward into 9
Build-Order-ordered commits on `main`, each one representing what
genuinely existed in the codebase at that step. `git log --oneline`
today still shows exactly that:

```
$ git log --oneline main
b01a9e8 Step 9: Redis — cache-aside, distributed lock, idempotent dedupe, rate limiting
95722d8 Step 8: saga pattern, choreography AND orchestration, built side by side
c459376 Step 7: analytics-service, windowed aggregations with Kafka Streams
633e780 Step 6: fraud-detection-service, OrderFlow's first Kafka Streams app
6ad7fea Step 5: transactional outbox pattern, solving the dual-write problem
4ec51ac Step 4: retry topics + Dead Letter Topic for poison messages
0ae9c0f Step 3: Avro + Schema Registry, replacing hand-copied JSON DTOs
53b11e3 Step 2: consumer groups + manual offset commits, scale inventory-service
859004a Step 1: single producer/consumer, plain JSON, order-created flow end to end
```

That reconstruction is itself worth understanding, because it used a
real, genuinely tricky git operation: **interactive rebase, to edit a
commit that already had four other commits stacked on top of it.**

### A real interactive rebase: inserting a forgotten file into commit 5

Partway through building the Step 1–9 history, `avro-schemas/order-status.avsc`
was discovered to be missing from commit 5 (it belonged there —
`order-status` is a Step 5 concept) but commits 6, 7, 8, and 9 had
ALREADY been created on top of it. Deleting and redoing four commits
just to fix one file in an earlier one would have been wasteful and
risky. Interactive rebase fixes exactly this: it can stop AT a specific
older commit, let you amend it, then automatically replay every commit
that came after it on top of the fixed version.

```bash
# Stop the rebase machinery right at commit 5 (SHA 27993b1), marking it
# "edit" instead of "pick" — every commit between the starting point
# (4ec51ac, Step 4) and HEAD gets replayed, but this ONE pauses first.
git rebase -i 4ec51ac
# (opens an editor listing every commit from 27993b1 onward as "pick";
#  changing 27993b1's line to "edit" and saving is what actually pauses there)
```

Real output:

```
Rebasing (1/3)Stopped at 27993b1...  # Step 5: transactional outbox pattern, solving the dual-write problem
You can amend the commit now, with

  git commit --amend

Once you are satisfied with your changes, run

  git rebase --continue
```

```bash
# Now HEAD is DETACHED, sitting exactly at commit 5's snapshot. Add the
# missing file and fold it into THIS commit, not a new one:
git add avro-schemas/order-status.avsc
git commit --amend --no-edit
```

This doesn't just edit the file — it computes a BRAND NEW commit SHA for
"Step 5" (since a commit's hash depends on its content), which means
every commit built on top of the old SHA is now pointing at a parent
that no longer exists in the branch's history. That's exactly what
`git rebase --continue` fixes next:

```bash
git rebase --continue
```

Real output:

```
Rebasing (2/3)Rebasing (3/3)Successfully rebased and updated refs/heads/main.
```

Git replayed Step 6, 7, and 8's commits one at a time on top of the
amended Step 5 — each one gets a NEW SHA too (a commit's hash includes
its parent's hash, so changing the parent changes every descendant's
hash, transitively, all the way to the tip). The commit MESSAGES and
diffs stayed identical; only the SHAs changed. `git log --oneline`
immediately after confirmed the fix landed and the chain was intact:

```
c459376 Step 7: analytics-service, windowed aggregations with Kafka Streams
633e780 Step 6: fraud-detection-service, OrderFlow's first Kafka Streams app
6ad7fea Step 5: transactional outbox pattern, solving the dual-write problem   <- new SHA, file added
4ec51ac Step 4: retry topics + Dead Letter Topic for poison messages           <- unchanged, rebase started here
```

**The one real danger this illustrates:** rebasing rewrites history —
every commit from the edited one forward gets a new identity. That's
completely safe on a branch nobody else has pulled yet (exactly this
repo's situation, still fully local at that point). It becomes actively
harmful the moment someone else has already based work on the OLD SHAs —
their history and yours would silently diverge. **Never rebase a branch
that's already been pushed and shared**, unless you explicitly coordinate
a force-push with everyone using it.

---

## Stage B — a real feature branch, start to finish (Step 10)

Once Steps 1–9 existed on `main`, every step after that followed a
deliberately different, more "enterprise" workflow: a feature branch per
step, real commits made AS the work happened (not reconstructed
afterward), a pull request, and an explicit merge — closer to how a real
team ships incremental work.

```bash
# Confirm main is clean and up to date before branching off it — branching
# from a dirty or stale main just relocates the problem, doesn't avoid it.
git status --short
git fetch origin
git rev-list --left-right --count main...origin/main   # 0  0 = fully in sync

# Create AND switch to the new branch in one command
git checkout -b feature/step-10-elasticsearch
```

Real output:

```
Switched to a new branch 'feature/step-10-elasticsearch'
```

`git checkout -b <name>` is shorthand for `git branch <name>` (create the
pointer, still on the old branch) immediately followed by
`git checkout <name>` (move `HEAD` to it). The modern equivalent,
`git switch -c <name>`, does the identical thing — `checkout` predates
`switch`/`restore` and still does both jobs (moving `HEAD` AND
discarding working-tree changes) that Git 2.23 later split into two less
overloaded commands.

### Committing real, incremental work on the branch

Four separate, real commits landed on this branch as the work actually
happened — not reconstructed afterward:

```
$ git log --oneline main..feature/step-10-elasticsearch
d7b2a29 Step 10 docs: root README + search-indexer-service README
d0573b4 Step 10: fix two real bugs found live-testing search-indexer-service
0151e7d Step 10: search-indexer-service — denormalized order search + Kibana feed
2617da8 Step 10 infra: Elasticsearch + Kibana
```

`main..feature/step-10-elasticsearch` (two dots) means "commits reachable
from the branch but NOT from `main`" — exactly the set of commits this
branch would add if merged. Compare with three dots below.

Each commit followed the same pattern used throughout this whole
project: stage exactly the files that belong together, write a message
explaining WHY, not just what:

```bash
git add pom.xml search-indexer-service/
git status --short          # review before committing — never blind-commit a broad `git add -A`
git commit -m "$(cat <<'EOF'
Step 10: search-indexer-service — denormalized order search + Kibana feed
...
EOF
)"
```

### Pushing and opening the PR

```bash
git push -u origin feature/step-10-elasticsearch
```

Real output:

```
remote: Create a pull request for 'feature/step-10-elasticsearch' on GitHub by visiting:
remote:      https://github.com/eakhtar1999/OrderFlow/pull/new/feature/step-10-elasticsearch
To https://github.com/eakhtar1999/OrderFlow.git
 * [new branch]      feature/step-10-elasticsearch -> feature/step-10-elasticsearch
branch 'feature/step-10-elasticsearch' set up to track 'origin/feature/step-10-elasticsearch'.
```

The `-u` (`--set-upstream`) flag is what makes plain `git push`/`git pull`
work on this branch from now on without repeating `origin
feature/step-10-elasticsearch` every time — it records the tracking
relationship once.

```bash
gh pr create --base main --head feature/step-10-elasticsearch \
  --title "Step 10: search-indexer-service (Elasticsearch + Kibana)" \
  --body "..."
```

Real result: `https://github.com/eakhtar1999/OrderFlow/pull/1` — a PR is
NOT a git object at all; it's a GitHub-side wrapper around "compare
these two branches," created via GitHub's API (which is all `gh` is
doing here — no different in principle from clicking "Compare & pull
request" on github.com).

---

## Stage C — reviewing before merging

```bash
gh pr diff 1                # the full diff, same as git diff main...HEAD would show
gh pr checks 1               # CI status — empty in this repo, no workflow configured yet
gh pr view 1 --json state,mergeable,mergeStateStatus
```

Real result: `{"mergeStateStatus":"CLEAN","mergeable":"MERGEABLE","state":"OPEN"}`
— GitHub had already computed that this branch merges into `main` with
no conflicts, before any merge command ran.

---

## Stage D — the actual merge, and why `--merge` was chosen over squash/rebase

```bash
gh pr merge 1 --merge
```

Real result: a genuine two-parent merge commit, `6747e09`:

```
$ git show --stat 6747e09 | head -5
commit 6747e092e6f82c65133e1026ad2d3cbb65f86fdc
Merge: b01a9e8 d7b2a29
Author: Ehtesham Akhtar
Date:   Sun Aug 2 16:30:10 2026 +0530

    Merge pull request #1 from eakhtar1999/feature/step-10-elasticsearch
```

`Merge: b01a9e8 d7b2a29` — TWO parent SHAs, right there. `b01a9e8` is
where `main` was before merging (Step 9's tip); `d7b2a29` is the feature
branch's own tip (the last of its 4 real commits). This is what makes
`--merge` fundamentally different from the other two GitHub merge
strategies:

| Strategy | What lands on `main` | What happens to the branch's own commits |
|---|---|---|
| **`--merge`** (used here) | One merge commit with two parents, PLUS all of the branch's original commits, individually, now reachable from `main` | Preserved exactly — SHAs, messages, and all, forever part of `main`'s history |
| **`--squash`** | ONE new commit containing the combined diff of everything on the branch | Discarded as individual commits — after deleting the branch, they become unreachable garbage (still exist briefly, collected eventually) |
| **`--rebase`** | Every branch commit individually, replayed on top of `main`, each getting a NEW SHA (same content, new parent) — no merge commit at all | Preserved in content and message, but under NEW SHAs; linear history, no branch/merge structure visible |

`--merge` was the deliberate choice for this repo specifically BECAUSE
the individual commits (infra → module → live-found bug fixes → docs)
tell a real, worth-preserving story — exactly the kind of thing worth
keeping visible for a portfolio repo built to demonstrate incremental,
real engineering work. Squashing would have collapsed 4 commits'
worth of "here's what I built, here's a real bug I found and fixed"
narrative into one undifferentiated diff.

### Cleaning up afterward

```bash
git checkout main
git pull origin main            # fast-forward — main had no NEW local commits of its own
git branch -d feature/step-10-elasticsearch   # -d refuses if the branch isn't fully merged; safe by default
git push origin --delete feature/step-10-elasticsearch
git fetch --prune               # clears any now-stale remote-tracking refs (origin/<deleted-branch>)
```

Real detail worth knowing: `git branch -d` (lowercase, "safe delete")
checks that the branch's commits are all reachable from your CURRENT
branch before deleting the local pointer — since this branch was merged
with `--merge` (not squashed), that check passes cleanly, and the four
commits stay permanently reachable through `6747e09`, the branch NAME is
just a label being removed, not the commits themselves. Deleting a
branch that was `--squash`-merged would need `-D` (force) — `-d` would
correctly refuse, because the branch's individual commits genuinely
aren't reachable from `main` in that case (only their squashed
combination is).

---

## Stage E — doing it again for Step 11 (proof it's a repeatable pattern, not a one-off)

The exact same sequence — branch, commit as you go, push, PR, review,
merge, clean up — ran again for Step 11, with real, different content:

```
$ git log --oneline main..feature/step-11-observability
2bf904b Step 11 docs: root README + inventory-service README
4267b2c Step 11: provisioned Grafana dashboard + real DLT counter metric
a7cec3f Step 11: fix missing consumer-lag metrics on inventory-service
5942fc8 Step 11: Micrometer + Prometheus metrics on all 8 services
24defcd Step 11 infra: Prometheus + Grafana + Jaeger
```

Five commits this time, not four — a genuinely separate commit
(`a7cec3f`) for a real bug found mid-step (inventory-service silently
missing consumer-lag metrics), landed as its OWN commit rather than
folded into whichever commit happened to be "current" when it was
found — the same discipline "stage exactly what belongs together" from
Stage B, applied consistently.

---

## Reference: commands used above, and a few worth knowing that this repo hasn't needed yet

### Inspecting history and changes

```bash
git log --oneline                          # compact, one line per commit
git log --oneline --graph --all            # ASCII graph of every branch's history
git log --oneline main..feature/x          # commits on feature/x, not yet on main
git log --oneline feature/x..main          # commits on main, not yet on feature/x (the reverse)
git log --oneline main...feature/x         # THREE dots: commits reachable from EITHER but not BOTH — symmetric difference
git diff                                   # working tree vs. the last commit (unstaged changes)
git diff --cached                          # staged changes vs. the last commit (what `git commit` would actually record)
git diff main...feature/x                  # what feature/x would introduce, computed from their COMMON ANCESTOR — the same comparison a GitHub PR diff shows, not a literal tip-to-tip diff
git show <sha>                             # one commit's full diff + metadata
git blame <file>                           # which commit last touched each line
```

### Branching

```bash
git branch -a                    # every branch, local + remote-tracking
git branch --show-current        # just the current one
git switch <name>                 # move HEAD to an EXISTING branch (modern, narrower than checkout)
git switch -c <name>              # create + switch in one step (modern equivalent of checkout -b)
```

### Stashing — not used in this repo yet, but essential when it's needed

Save uncommitted work-in-progress without committing it, to switch
branches or pull cleanly, then bring it back later:

```bash
git stash push -u -m "description"   # -u also stashes untracked files, not just tracked changes
git stash list
git stash pop                        # re-apply the most recent stash AND remove it from the stash list
git stash apply                      # re-apply WITHOUT removing it (useful if you want to apply the same stash twice)
```

### Cherry-picking — not used in this repo yet

Copy ONE specific commit from anywhere onto your current branch, without
merging or rebasing everything else around it — useful for "I need just
that one bugfix commit from another branch, not its whole history":

```bash
git cherry-pick <sha>
```

### Reflog — the actual safety net behind "undo" in git

Every commit `HEAD` has ever pointed to on THIS machine, even ones no
branch currently points to (after a reset, an amend, a rebase) —
real output from this repo's own reflog:

```
$ git reflog | head -6
2bf904b HEAD@{0}: commit: Step 11 docs: root README + inventory-service README
4267b2c HEAD@{1}: commit: Step 11: provisioned Grafana dashboard + real DLT counter metric
a7cec3f HEAD@{2}: commit: Step 11: fix missing consumer-lag metrics on inventory-service
5942fc8 HEAD@{3}: commit: Step 11: Micrometer + Prometheus metrics on all 8 services
24defcd HEAD@{4}: commit: Step 11 infra: Prometheus + Grafana + Jaeger
6747e09 HEAD@{5}: checkout: moving from main to feature/step-11-observability
```

If a rebase or reset ever goes wrong, `git reset --hard HEAD@{N}` (using
whatever entry was correct BEFORE the mistake) recovers it — the reflog
is local-only (never pushed, never shared) and expires old entries after
~90 days by default, but it's the real reason "I force-pushed/rebased
something and lost work" is almost always recoverable if caught in time.

### Tags — not used in this repo yet

An immutable, named pointer to one specific commit — unlike a branch, a
tag never moves. The natural next step for this repo once Build Order
Step 13 (the system design doc) lands would be tagging that point as a
release marker:

```bash
git tag -a v1.0-build-order-complete -m "All 13 Build Order steps done"
git push origin v1.0-build-order-complete
```

### Safety and undo — the commands that make mistakes reversible

| Situation | Command | What it actually does |
|---|---|---|
| Unstage a file, keep its edits | `git restore --staged <file>` | Moves it out of the index, working tree untouched |
| Discard uncommitted changes to one file | `git restore <file>` | Working tree reverts to the last commit's version — DESTRUCTIVE, no undo except reflog-adjacent tricks that don't really apply to working-tree-only changes |
| Undo the last commit, keep the changes staged | `git reset --soft HEAD~1` | Moves the branch pointer back one commit; index and working tree untouched |
| Undo the last commit, keep changes unstaged | `git reset HEAD~1` (mixed, the default) | Same, but also unstages |
| Undo the last commit AND discard its changes entirely | `git reset --hard HEAD~1` | The genuinely destructive one — combine with `git status` first, always, per this project's own safety conventions |
| Undo a commit that's ALREADY been pushed/shared | `git revert <sha>` | Creates a NEW commit that reverses the old one's changes — safe for shared history, since it adds rather than rewrites |

---

## 🧠 Interview Q&A — twisted questions designed to confuse you

**Q1. You ran `git checkout -b feature/x` then made 3 commits, then
realized you forgot to branch off an up-to-date `main` — you'd branched
off `main` from an hour before a teammate's merge. What actually
happened to your 3 commits when you eventually merge feature/x back?**
A: Nothing bad, automatically — your 3 commits are still valid, they
just have an OLDER commit as their common ancestor with the current
`main`. Merging (or rebasing) will reconcile the divergence; a real
conflict only arises if your commits and the teammate's touched
overlapping lines. The commits themselves were never "wrong" for being
based on stale `main` — that's the entire point of branches existing.

**Q2. This repo used `git rebase -i` to edit commit `27993b1` after 3
commits already existed on top of it. Why didn't that corrupt the 3
later commits?**
A: It DID change them — every commit downstream of an edited one gets a
brand NEW SHA, because a commit's hash is computed from its content
INCLUDING its parent's hash. What DIDN'T change: their diffs and
messages. `git rebase --continue` replays each one's ORIGINAL diff
against the new parent automatically — the content is preserved, only
the identity (SHA) changes. This is exactly why rebasing already-pushed,
already-shared commits is dangerous: anyone who already has the OLD SHAs
now has a divergent history from yours, even though the actual code
content is identical.

**Q3. `git branch -d feature/step-10-elasticsearch` succeeded without
`-D` (force) after a `--merge` PR, but would have REQUIRED `-D` after a
`--squash` PR. Why does git know the difference?**
A: `-d` checks whether every commit on the branch is reachable from your
current `HEAD` via `git merge-base --is-ancestor`. After `--merge`,
the branch's original commits are literally parents of the merge commit
now on `main` — reachable, so `-d` passes. After `--squash`, `main` only
has ONE new commit (a fresh SHA, computed from the combined diff) — the
branch's ORIGINAL commits were never actually merged in as themselves,
so git correctly considers them "not merged" and refuses the safe
delete.

**Q4. `main...feature/x` (three dots) and `feature/x...main` (arguments
swapped) — do these two commands show different things?**
A: No — three-dot diff/log syntax is symmetric by design (it's the
symmetric difference: commits reachable from EITHER ref but not both).
Contrast with TWO dots (`main..feature/x`), which IS order-sensitive —
`main..feature/x` means "on feature/x, not on main," while
`feature/x..main` means the reverse.

**Q5. `git push -u origin feature/step-10-elasticsearch` was run once.
Every push after that just used `git push` with no arguments. What
would happen if a SECOND developer, on a fresh clone, ran `git push`
immediately after `git checkout -b feature/step-10-elasticsearch` locally
(same branch name, no `-u` yet)?**
A: It would fail with "no upstream branch" — `-u` isn't automatic per
branch NAME, it's a tracking relationship recorded per LOCAL branch
object, and a fresh `checkout -b` creates a brand-new local branch
object with no tracking info yet, even if a remote branch with the same
name exists. They'd need `git push -u origin feature/step-10-elasticsearch`
(or `git push --set-upstream`) themselves, once, on their own clone.
