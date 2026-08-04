# Branching Model

Three long-lived branches, each mapped to a real Databricks Asset Bundle
target (`databricks.yml`) and validated independently in CI
(`.github/workflows/ci.yml`):

| Branch | Bundle target | Catalog | Purpose |
|---|---|---|---|
| `main` | `dev` | `mdm_dq_demo` | Trunk. All feature/hotfix branches merge here first. Applied and live today. |
| `test` | `test` | `mdm_dq_test` | Staging. Promoted from `main` once it's stable enough to soak. |
| `prod` | `prod` | `mdm_dq_prod` | Production. Promoted from `test` only. |

Naming follows this workspace's existing convention (`lakehouse_demo`,
`lakehouse_demo_test`, `lakehouse_demo_prod` are already set up the same
way).

**`mdm_dq_test` and `mdm_dq_prod` don't exist yet** — only `mdm_dq_demo` has
been provisioned via `terraform apply` (see `terraform/`). The `test`/`prod`
bundle targets and branches exist so the workflow is ready; actually
deploying to them means provisioning their catalogs/schemas/Volumes first
(copy the pattern in `terraform/catalog.tf`/`volumes.tf`, pointed at a new
catalog name — or extend `terraform/` to take the catalog as a variable
across environments, which it doesn't today).

## Short-lived branches

Not pre-created — cut one per piece of work, off `main`, delete it after
merging:

- **`feature/<short-description>`** — new functionality. Branch from
  `main`, open a PR back into `main`. Example: `feature/product-entity`.
- **`hotfix/<short-description>`** — urgent production fix. Branch from
  `prod` (not `main` — `main` may already have unreleased changes you don't
  want to drag into an emergency fix), PR back into `prod`, then
  **immediately** back-merge `prod` into `test` and `main` so the fix isn't
  silently lost on the next normal promotion.

## Promotion flow

```
feature/* ──PR──> main ──promote──> test ──promote──> prod
                                                          ↑
                                              hotfix/* ──PR┘ (then back-merge → test, main)
```

"Promote" means merging (or fast-forwarding) `main` into `test`, and
`test` into `prod` — via PR, not a direct push, so CI validates the target
environment's bundle config before it lands (`bundle-validate` in CI
resolves `main`→`dev`, `test`→`test`, `prod`→`prod` automatically based on
which branch is being pushed to).

## Recommended GitHub branch protection

Not set up by this commit — repo-admin-level settings, your call to enable
under **Settings → Branches**:
- Require a PR (no direct pushes) on `test` and `prod` at minimum — `main`
  can stay more permissive if you're the only contributor right now.
- Require the CI checks (`python-tests`, `terraform-validate`,
  `bundle-validate`) to pass before merging.
