# GitHub Actions Fundamentals — read this before `.github/workflows/*.yml`

Same spirit as [`docs/aws-cloud-deployment.md`](aws-cloud-deployment.md):
real files from this repo as the running example, deep enough that the
pattern transfers to a different project. Read this once, end to end,
before touching any workflow file — the four workflows
(`backend-ci.yml`, `backend-deploy.yml`, `frontend-ci.yml`,
`frontend-deploy.yml`) each carry their own inline comments explaining
what THAT file specifically does, but assume you already know the
vocabulary this document defines.

---

## 1. Anatomy of a workflow file

A workflow is one YAML file under `.github/workflows/`. GitHub watches
that directory; any file there is a workflow, auto-registered, no
separate "enable this workflow" step needed.

```yaml
on:                       # WHEN this workflow runs
  push:
    branches: [main]
    paths: ['frontend/**']
  workflow_dispatch: {}    # a manual "Run workflow" button in the Actions tab

jobs:                      # ONE OR MORE independent units of work
  build:
    runs-on: ubuntu-latest  # the VM ("runner") this job executes on
    steps:                  # an ORDERED list of things this job does
      - uses: actions/checkout@v4      # run someone else's packaged action
      - run: npm ci                     # run a raw shell command
```

- **`on`** — the trigger. Can be a single event (`push`), multiple events,
  or event + filters (`branches`, `paths`). `workflow_dispatch` is the
  "someone clicks a button" trigger — no `push`/`pull_request` involved at
  all, which is exactly why this project's two `*-deploy.yml` workflows
  use it (see §6).
- **`jobs`** — each job gets its **own fresh virtual machine**. Two jobs
  in the same workflow do NOT share filesystem state unless you
  explicitly pass data between them (§4). This matters: `backend-ci.yml`
  runs Maven in one job; that job's `target/` directories vanish the
  moment the job ends.
- **`steps`** — run sequentially inside one job, on the same VM, and DO
  share filesystem state with each other (an earlier step's `npm ci` output
  is still on disk for a later step's `npm test`).
- **`uses` vs `run`** — `run` executes a literal shell command on the
  runner. `uses` invokes a published, reusable **Action** (a packaged unit
  of steps someone else wrote — `actions/checkout`, `actions/setup-java`,
  `aws-actions/configure-aws-credentials`). Prefer `uses` for anything
  that already has a well-maintained action; `run` for anything specific
  to this repo (`mvn -B verify`, `npm run build`).

## 2. Runners

`runs-on: ubuntu-latest` provisions a brand-new, ephemeral Ubuntu VM for
that job — nothing persists between runs (no cache, no leftover files)
unless a step explicitly saves it (§4). Every one of this project's
workflows uses `ubuntu-latest`; there's no reason here to reach for a
self-hosted runner or a different OS.

## 3. `permissions:` and why `id-token: write` matters

By default, every workflow run gets an automatic, short-lived
`GITHUB_TOKEN` (scoped to THIS repo, expires when the job ends) — used
for things like checking out code or commenting on a PR. Its default
permissions are broad-ish; the `permissions:` block narrows that:

```yaml
permissions:
  contents: read      # only needs to check out code, nothing else
  id-token: write      # <-- the one that makes OIDC possible
```

`id-token: write` is NOT about `GITHUB_TOKEN` at all — it's what allows
the job to request a **separate**, OIDC-specific token from GitHub's own
token service, the one `aws-actions/configure-aws-credentials` (§6)
actually uses to talk to AWS. Without this line, the OIDC step fails
outright — this is a real, easy-to-forget prerequisite, not an
optional hardening step.

## 4. Sharing data between jobs: caching vs. artifacts

Two different mechanisms, solving two different problems — mixing them up
is the most common beginner mistake:

- **Cache** (`actions/cache`, or the built-in cache option on
  `setup-java`/`setup-node`) — for **dependencies that don't change
  between runs** (Maven's `~/.m2` repository, npm's `node_modules`
  download cache). Speeds up a job; the job still redownloads/rebuilds if
  the cache is missing or stale. `backend-ci.yml` uses `setup-java`'s
  built-in `cache: maven` — one line, no separate `actions/cache` step
  needed, because that's a solved problem for the common case.
- **Artifacts** (`actions/upload-artifact` / `actions/download-artifact`)
  — for **this run's actual build output**, passed from one job to
  another. If `backend-deploy.yml` ever split "build the jar" and "build
  the Docker image" into two separate jobs, the jar would need to travel
  between them as an artifact — a cache is the wrong tool for that
  (caches are best-effort and keyed by content hash, not "the exact thing
  job A just produced").

This project's workflows are simple enough (one job each) that artifacts
aren't needed yet — flagged here because the distinction matters the
moment a workflow grows a second job.

## 5. `needs`, `if`, and matrix builds — and why this project skips matrices

- **`needs: [job-a]`** on a job makes it wait for `job-a` to finish
  (and by default, skip if `job-a` failed) before starting.
- **`if:`** — a conditional gate on a job or a step, evaluated against
  the workflow's context (branch name, event type, a previous step's
  outcome).
- **Matrix builds** (`strategy: matrix: ...`) — run the SAME job
  definition multiple times with different input values (e.g., once per
  service, once per Node version) in parallel.

**This project deliberately does NOT matrix `backend-ci.yml` across the 8
services.** OrderFlow is one Maven multi-module reactor (root `pom.xml`),
not 8 independent projects — `docs/aws-cloud-deployment.md`'s Phase 1
already explains why every Dockerfile builds from the repo root, and the
same fact applies here: a single `mvn -B -ntp verify` at the repo root
builds and tests every module in the correct dependency order in one
pass. An 8-way matrix would mean either 8x redundant `dependency:go-
offline` downloads, or fighting Maven's reactor to build one module in
isolation — real complexity with no corresponding benefit for a project
this shape. `backend-deploy.yml` *does* let you target one specific
service (via `workflow_dispatch` input, §6) — that's a manual choice at
deploy time, not a matrix.

## 6. `workflow_dispatch` and OIDC — how this project deploys to AWS

Both `*-ci.yml` workflows trigger on `push`/`pull_request` — fully
automatic, and deliberately request **no AWS credentials at all** (no
`permissions: id-token: write`, no AWS step). They only build/lint/test;
there's nothing there that could touch billable infrastructure even by
accident.

Both `*-deploy.yml` workflows trigger ONLY on `workflow_dispatch` — a
person has to open the Actions tab and click "Run workflow." This
mirrors `cloud/scripts/up.sh`/`down.sh`'s existing philosophy: paid AWS
infrastructure changes happen because someone deliberately triggered
them, never as a side effect of an automatic merge.

Inside a deploy workflow:

```yaml
permissions:
  id-token: write
  contents: read

steps:
  - uses: aws-actions/configure-aws-credentials@v4
    with:
      role-to-assume: arn:aws:iam::027677879393:role/orderflow-github-deploy-role
      aws-region: us-east-1
```

`configure-aws-credentials` requests GitHub's OIDC token (allowed because
of `id-token: write`), sends it to AWS STS's `AssumeRoleWithWebIdentity`
API, and AWS checks it against `orderflow-github-deploy-role`'s trust
policy (`cloud/cdk/lib/github-oidc-stack.ts`) — specifically, that the
token's `sub` claim equals `repo:eakhtar1999/OrderFlow:ref:refs/heads/main`.
Only if that matches does AWS hand back temporary credentials (~1hr TTL),
which every subsequent AWS CLI/SDK call in that job then uses
automatically. Nothing about this role's ARN or trust condition is a
secret — the actual security boundary is that AWS itself verifies the
cryptographic signature on GitHub's token before ever consulting the
trust policy, not that the ARN is hidden.

**Why this beats storing `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` as
GitHub Secrets**: those would be long-lived and valid from anywhere,
forever, until manually rotated. A leaked OIDC-derived credential expires
on its own within the hour, and can only ever have been minted by a
workflow run that GitHub itself vouches for, from this exact repo and
branch.

## 7. Secrets vs. variables

- **Secrets** (repo/environment Settings → Secrets) — encrypted, never
  printed in logs (GitHub automatically redacts a secret's value if it
  ever appears in output). This project's deploy workflows don't need
  any AWS secret (OIDC replaces it, §6) — the only thing worth a Secret
  here would be something like a third-party API key, which OrderFlow
  doesn't currently have.
- **Variables** (repo/environment Settings → Variables, or `vars.X` in a
  workflow) — plain text, visible in the UI and in logs. Used for
  non-sensitive per-environment config: `frontend-deploy.yml` reads the
  five `VITE_*_API_URL` values (see `frontend/.env.example`) from
  repository variables, since a production ALB DNS name isn't a secret —
  it's a public hostname, just one that shouldn't be hardcoded into a
  workflow file.

## 8. Environments and protection rules (available, not used here)

GitHub Environments (Settings → Environments) let a job target a named
environment (`environment: production`) and attach protection rules —
most commonly, "require a specific person to approve before this job
runs," which pauses the job in a waiting state until approved in the
Actions UI. This project's deploy workflows use plain
`workflow_dispatch` instead (the person triggering the run already IS the
approval) — documented here because it's a real, useful pattern for a
team setting (multiple people can push, but only an approved reviewer
can let a deploy proceed), just not the shape this solo project needs.

---

## Phase 0 setup — do this once, by hand, before any deploy workflow can succeed

These are real AWS-account changes (not free to undo casually), so
they're a deliberate manual step, not something a workflow does for you
on first run:

```bash
# 1. Bootstrap CDK in this account/region (creates the CDKToolkit stack —
#    an S3 staging bucket + IAM roles CDK itself needs to deploy anything).
#    One-time, per account+region.
cd cloud/cdk
AWS_PROFILE=ea-admin node_modules/.bin/cdk bootstrap aws://027677879393/us-east-1

# 2. Deploy the OIDC provider + deploy role, and the frontend's S3/CloudFront
#    infrastructure (both are cheap/free to run, unlike the ECS stacks).
AWS_PROFILE=ea-admin node_modules/.bin/cdk deploy OrderFlowFrontendStack OrderFlowGithubOidcStack

# 3. Nothing to copy-paste: both deploy workflows' `DEPLOY_ROLE_ARN` is
#    already the real, deterministic ARN
#    (arn:aws:iam::027677879393:role/orderflow-github-deploy-role) —
#    GithubOidcStack pins that exact role name, so the ARN was
#    predictable before the stack was ever deployed. Step 2 is what
#    makes that ARN start actually resolving to something.
```

After that, `.github/workflows/*-deploy.yml` become safe to run from the
Actions tab. One more one-time step, in the GitHub UI, not via CDK: add
the five `VITE_*_API_URL` repository variables (Settings → Secrets and
variables → Actions → Variables) that `frontend-deploy.yml` reads — see
`frontend/.env.example` for what each one means; production values look
like `https://<alb-dns-from-app-services-stack-output>/orders` (etc.),
using the ALB DNS `docs/aws-cloud-deployment.md`'s own `aws elbv2
describe-load-balancers` command prints.
