# KEP-XYZ: Dedicated Repository for Kubeflow Pipelines Components & Pipelines

<!-- toc -->

- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Repository Layout](#repository-layout)
    - [Core (components and pipelines)](#core-components-and-pipelines)
    - [Third-Party (third_party)](#third-party-third_party)
  - [Artifact Metadata Schema](#artifact-metadata-schema)
  - [Standardized README Templates](#standardized-readme-templates)
  - [Linting & Continuous Integration](#linting--continuous-integration)
  - [Pytest Structure and Local Execution](#pytest-structure-and-local-execution)
  - [Maintenance Automation](#maintenance-automation)
  - [Onboarding & Documentation](#onboarding--documentation)
  - [Packaging & Release Management](#packaging--release-management)
  - [Governance & Tiers](#governance--tiers)
  - [Open Questions](#open-questions)
- [Design Details](#design-details)
  - [Implementation Phases](#implementation-phases)
  - [Rollout and Migration](#rollout-and-migration)
- [Risks and Mitigations](#risks-and-mitigations)
- [Test Plan](#test-plan)
  - [Unit & Compile Checks](#unit--compile-checks)
  - [Continuous Integration](#continuous-integration)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Separate Repositories](#separate-repositories)
  - [Keep the Existing `components` Directory in the KFP Repo](#keep-the-existing-components-directory-in-the-kfp-repo)
  <!-- /toc -->

## Summary

Establish a dedicated Kubeflow Pipelines (KFP) repository\* that hosts reusable components and full pipelines under a
consistent structure, governance policy, and release cadence. The repository will package officially supported assets as
a Python distribution for easy consumption, while providing a parallel space for vetted third-party contributions. The
project introduces standardized metadata, documentation, testing, and maintenance automation to make components
discoverable, reliable, and safe to adopt.

\*Working title `kubeflow/kfp-components`; the final repository name will be confirmed during implementation.

## Motivation

Kubeflow Pipelines currently ships sample components in-tree under `components/`, making reuse and contribution
difficult because:

- Assets are versioned with the main KFP repo, so release timing is tied to the entire project.
- Documentation is inconsistent and buried.
- There is no uniform metadata that tells users about compatibility, dependencies, or support status.
- CI coverage is uneven, so users do not know if a component still works.
- Third-party components mix with community-maintained artifacts without clear ownership or expectations.
- Components are not published as a Python package today, so consumption requires cloning the monorepo or copying
  snippets manually.

A dedicated repository with purpose-built tooling will make it easier for users to discover, evaluate, and integrate KFP
assets while allowing the community to maintain them at its own cadence.

This catalog also creates a durable bridge between Kubeflow Pipelines and the broader Kubeflow ecosystem: it gives the
SDK and API server a canonical source of reusable building blocks, provides SIGs with a venue to showcase
Kubernetes-native MLOps patterns, and offers users a distribution channel that evolves independently from the core KFP
release train. Centralizing community-supported assets in one place keeps the SDK surface area lean while encouraging
experimentation in third-party areas that can later graduate into core.

### Goals

1. Move reusable components and pipelines into a dedicated GitHub repository with clear structure and governance.
2. Provide standardized metadata, documentation, and testing requirements for every asset.
3. Ship an installable Python package for core (community-maintained) artifacts that is versioned to match Kubeflow
   releases.
4. Maintain a parallel, clearly demarcated area for third-party contributions, shipped as its own Python package that
   tracks the same release cadence as the core catalog.
5. Automate maintenance (e.g. stale component detection, dependency validation) to keep the catalog healthy.
6. Provide developer onboarding materials and guidance for agents generating components/pipelines.

### Non-Goals

1. Operate turnkey build or publishing pipelines for third-party maintainers. We supply validation tooling and guidance,
   but external teams continue to manage their own release automation.
2. Backport historical component versions; only actively maintained assets will be migrated.
3. Remove the existing `components/` directory in the main KFP repo without notice. We will announce the new repository,
   point contributors at the new onboarding documentation, and give the community a minimum of 90 days to move their
   assets before deleting the legacy directory.
4. Vendor specific components. Vendors are highly encouraged to use the same repository structure in their own
   repository.

## Proposal

### Repository Layout

Create a new repository (e.g. `kubeflow/kfp-components`) with the following top-level layout:

```
root
├── components
│   ├── README.md (catalog landing page)
│   ├── __init__.py (auto-imports all components for clean `from kfp_components import training` usage)
│   ├── training/
│   │   ├── README.md (category index listing each component with summaries/links)
│   │   ├── __init__.py (re-exports all components in this category)
│   │   └── <component-name>/
│   │       ├── __init__.py (exposes the component entrypoint for imports)
│   │       ├── component.py
│   │       ├── metadata.yaml
│   │       ├── README.md
│   │       ├── OWNERS
│   │       ├── example_pipelines.py
│   │       ├── tests/
│   │       │   └── test_component.py
│   │       └── <supporting_files>
│   └── ... (other categories: evaluation/, data_processing/, etc.)
├── pipelines
│   ├── README.md (catalog landing page)
│   ├── __init__.py (auto-imports all pipelines for `from kfp_components import pipelines.training` usage)
│   ├── training/
│   │   ├── README.md (category index listing each pipeline with summaries/links)
│   │   ├── __init__.py (re-exports all pipelines in this category)
│   │   └── <pipeline-name>/
│   │       ├── __init__.py (exposes the pipeline entrypoint)
│   │       ├── pipeline.py
│   │       ├── metadata.yaml
│   │       ├── README.md
│   │       ├── OWNERS
│   │       ├── tests/
│   │       │   └── test_pipeline.py
│   │       └── <supporting_files>
│   └── ... (other categories: evaluation/, data_processing/, etc.)
├── third_party
│   ├── pyproject.toml (third-party package build definition)
│   ├── __init__.py
│   ├── components
│   │   ├── README.md (catalog landing page)
│   │   ├── __init__.py
│   │   └── ... (mirrors `components/` structure)
│   └── pipelines
│       ├── README.md (catalog landing page)
│       ├── __init__.py
│       └── ... (mirrors `pipelines/` structure)
├── docs/
│   ├── ONBOARDING.md
│   ├── CONTRIBUTING.md
│   ├── AGENTS.md
│   ├── GOVERNANCE.md
│   └── BESTPRACTICES.md
├── scripts/
│   ├── validate_metadata.py
│   ├── generate_readme.py
│   ├── update_init_imports.py (auto-generates category `__init__.py` import stubs; enforced by CI)
│   └── verify_dependencies.py
└── pyproject.toml (core package)
```

#### Core (`components/` and `pipelines/`)

- Owned and maintained by the Kubeflow community.
- Each category directory hosts one component/pipeline per subdirectory.
- Every asset must include `component.py` or `pipeline.py`, `metadata.yaml`, `README.md`, `OWNERS`, and optional
  supporting files. The `OWNERS` file empowers the owning team to review changes, update metadata, and manage lifecycle
  tasks without central gatekeeping.
- Optional internal unit tests must live under a `tests/` subdirectory to avoid clutter.
- Core ownership implies Kubeflow release alignment and adherence to community support expectations.

#### Third-Party (`third_party/`)

- Not maintained by the Kubeflow SIGs.
- Mirrors the full structure of core assets (`components/` and `pipelines/` subtrees, category indexes, per-asset
  folders).
- Packaged separately via the `third_party/pyproject.toml` definition so consumers opt in explicitly.
- Metadata must flag `tier: third_party` and list external support channels; Kubeflow community reviews ensure
  lint/tests pass but does not guarantee functionality.
- Third-party assets remain the responsibility of their listed owners; Kubeflow maintainers provide validation
  infrastructure only.

### Artifact Metadata Schema

Each component or pipeline ships a `metadata.yaml` with the following validated fields:

```yaml
tier: core | third_party
name: <string>
min_kfp_version: 2.12 # minimum compatible Kubeflow Pipelines minor version (no patch)
stability: alpha | beta | stable
dependencies:
  external_services:
    - Kubeflow Trainer v2.1.0
lastVerified: 2025-03-15T00:00:00Z
ci:
  skip_dependency_probe: false
  pytest: optional
links:
  documentation: https://kubeflow.org/components/<name>
  issue_tracker: https://github.com/kubeflow/kfp-components/issues
```

Validation rules:

- `lastVerified` must be RFC 3339 and within one year of the current date. Automation enforces reminders (see
  [Maintenance Automation](#maintenance-automation)).
- `min_kfp_version` is expressed as `major.minor` for the minimum Kubeflow Pipelines version needed for this component.
- Components/pipelines that declare dependencies must include a `requirements.txt` in their directory. CI parses both
  the file and any `packages_to_install` lists in DSL decorators to ensure they stay in sync for Dependabot monitoring.
- `external_services` is a free-form list describing human-managed dependencies (e.g. `Kubeflow Trainer v2.1.0`);
  entries are surfaced verbatim in generated READMEs.
- `skip_dependency_probe` allows opting out of dependency installation when native extensions or heavy dependencies make
  sandbox installs infeasible; justification is required in the PR description.
- Compile validation always runs; assets must compile successfully via `kfp.compiler`.
- Additional optional fields (e.g. `tags`) can be added later but must pass schema validation.

### Standardized README Templates

Each component/pipeline directory includes a `README.md` generated from a template and auto-populated with docstring
metadata (every `metadata.yaml` field except `ci` is rendered verbatim). Component README files additionally embed
details from a required colocated `example_pipelines.py` module (which may expose multiple sample pipelines), while
pipeline README files may opt-in to the usage section when they are intended for reuse as nested components:

````markdown
# <Component Name> ✨

## Overview 🧾

<short description pulled from module docstring>

## Inputs 📥

| Name    | Type   | Description           |
| ------- | ------ | --------------------- |
| input_a | String | Path to input dataset |
| ...     | ...    | ...                   |

## Outputs 📤

| Name  | Type  | Description            |
| ----- | ----- | ---------------------- |
| model | Model | Trained model artifact |

## Usage Example (components only) 🧪

```python
# example_pipelines.py
from kfp import dsl
from kfp_components import training.my_component

@dsl.pipeline(name="example-pipeline")
def pipeline():
    training.my_component(dataset_path="gs://bucket/data.csv")
```

## Metadata 🗂️

- Tier: core
- Name: training.my_component
- Stability: Beta
- Minimum KFP Version: 2.12.0
- Owners:
  - @kubeflow/triage-ml
- Last Verified: 2025-03-15

## Additional Notes 📝

- Dependencies:
  - pandas==2.2.1
  - google-cloud-bigquery>=3.0.0
- External Services:
  - Kubeflow Trainer v2.1.0
- Base Image: ghcr.io/my-project/my-component-base:latest
- Documentation: https://kubeflow.org/components/training.my_component
- Issue Tracker: https://github.com/kubeflow/kfp-components/issues
````

A script (`scripts/generate_readme.py`) will introspect the component function signature and docstrings to populate the
inputs / outputs tables and example usage, and will also build category index READMEs; CI re-runs this script to prevent
README drift. Contributors may append custom sections below the autogenerated content—any text placed after a marker
(e.g. `<!-- custom-content -->`) is preserved verbatim across regeneration.

### Linting & Continuous Integration

Baseline CI workflow runs on every PR affecting the repository:

1. `scripts/validate_metadata.py` ensures the YAML schema is satisfied, `lastVerified` is fresh, and an OWNERS file
   exists.
2. Markdown lint (enforce repository conventions for README and docs formatting; scoped to PRs that touch markdown).
3. Black formatting (`black --check --line-length 120`).
4. Docstring lint verifying Google-style docstrings (e.g. `pydocstyle --convention=google`) and enforcing docstrings on
   every `dsl.component` or `dsl.pipeline`-decorated function.
5. Static import guard: ensure only stdlib imports appear at module top level; third-party imports must live inside the
   component function body (custom script using `ast`).
6. Compile check: whenever an asset changes, run `kfp.compiler` to ensure the component/pipeline compiles without
   execution.
7. Dependency probe: for assets that changed and did not set `skip_dependency_probe`, create a temporary virtual
   environment, run `pip install --dry-run -r requirements.txt`, and ensure installation succeeds.
8. Pytest discovery: run `pytest` for any `tests/` directories that correspond to changed assets. Tests must stay
   lightweight and avoid cluster dependencies; enforce a two-minute timeout per test via `pytest-timeout`.
9. Example pipelines check: import `example_pipelines.py` modules and run `kfp.compiler` on their exported pipelines to
   ensure the documented samples compile cleanly.

README drift check is handled automatically by the linting workflow described above (skips when README-related files are
unmodified).

### Pytest Structure and Local Execution

- Optional unit tests for utility functions or local execution must live under `tests/` within each asset directory
  (`components/<category>/<name>/tests/`).
- Tests can assume only `pytest` and the Python standard library; runtime dependencies from `requirements.txt` are
  installed during component execution, not test collection.
- Encourage contributors to include local execution smoke tests (e.g. verifying the component function returns expected
  outputs) but avoid long-running or cloud-dependent tests.

### Maintenance Automation

Scheduled automation runs weekly:

1. `lastVerified` sweep: flag assets where `lastVerified` is older than nine months. Create a GitHub issue at the
   nine-month mark tagging listed owners and label as `needs-verification`.
2. Removal countdown: once an asset is 12 months past `lastVerified`, automation opens a PR removing the asset from the
   catalog (and packages). It references the issue opened three months earlier. Owners can refresh verification by
   updating metadata and re-running CI.
3. Dependabot configuration to open pull requests when `requirements.txt` files change; owners review, run validation,
   and update `lastVerified` once verification succeeds.
4. Security watchdog: flag components/pipelines whose dependencies are affected by critical CVEs. If remediation PRs do
   not land within 90 days, automation proposes removal of the affected asset from the catalog and packages.

Automation scripts will live under `scripts/` and will be orchestrated via GitHub Actions cron jobs.

### Onboarding & Documentation

- `docs/ONBOARDING.md`: step-by-step guide for new contributors (repo setup, virtualenv, running lint/tests, submitting
  PRs).
- `docs/BESTPRACTICES.md`: authoring guidelines for components and pipelines (patterns, anti-patterns, validation tips);
  `docs/AGENTS.md` must stay in sync with these recommendations for automated tooling.
- `docs/AGENTS.md`: guidance for code-generation agents emphasizing reuse of existing components, best practices for new
  contributions, and instructions on selecting between core and third-party assets.
- `docs/GOVERNANCE.md`: clarifies governance model, including release managers, approvers, and the policy for moving
  assets between tiers.
- Category `README.md` files act as indices. Each lists the components/pipelines in that category, provides one-line
  summaries, and links to the corresponding asset directories; these files are generated automatically and kept fresh by
  the README automation described above.
- OWNERS files are documented as the primary self-service mechanism for teams to manage their assets (approvals,
  metadata refreshes, and lifecycle decisions). Initial OWNERS entries must include the PR author; future updates are
  approved by any existing owner in that file. Metadata `owners` entries are validated to match the OWNERS roster so
  that downstream tooling (catalog docs, potential website integrations) can display consistent contact points.
- The Kubeflow website will be updated to promote and link to this repository once the initial release is available.
  Longer term, we can add automation that renders and syncs catalog docs on the website.

### Packaging & Release Management

- Core assets packaged as `kfp-components` (`pyproject.toml`). Packaging scripts ingest metadata and generate import
  stubs for each component/pipeline, enabling `from kfp_components.training import my_component` imports.
- Third-party assets packaged separately as `kfp-components-third-party` (built from `third_party/pyproject.toml`).
  Shared `requirements.txt` files keep packaging and runtime environments aligned.
- Components and pipelines are distributed as module packages (not namespace packages); category `__init__.py` files
  auto-import all assets for ergonomic usage. Namespace packages were rejected to keep imports explicit and avoid
  packaging complexity across multiple wheels.
- Catalog releases follow semantic versioning with the `major` and `minor` numbers tracking Kubeflow. Patch versions
  capture catalog-only fixes (metadata refreshes, documentation updates) that do not change compatibility guarantees.

### Governance & Tiers

- **Core Tier:**
  - Maintained by Kubeflow SIGs; must meet verification SLA (update `lastVerified` annually) and pass all CI.
  - OWNERS must include at least one Kubeflow SIG owner/team for accountability.
  - Core assets reflect Kubeflow community ownership and support expectations.
  - OWNERS files on each asset enable self-service maintenance: listed owners approve changes, refresh metadata, and
    coordinate issue triage without waiting on the repo-wide maintainers.
  - Removal requires a documented deprecation period spanning at least two Kubeflow releases, with clear communication
    to users.
- Repository stewardship: Kubeflow Pipelines maintainers serve as primary owners for governance, release cadence, and
  escalation handling.
- **Third-Party Tier:**
  - Not maintained by Kubeflow SIGs.
  - Hosted in `third_party/`; contributions vetted for lint/tests only.
  - OWNERS list must point to external maintainers; Kubeflow community does not guarantee functionality.
  - Packaged separately to allow users to opt in.
  - Support, maintenance cadence, and release decisions remain the responsibility of the third-party owners, including
    removal when they deem appropriate.

Movement between tiers is proposed via pull request by Kubeflow community members and reviewed by SIG Pipelines.

### Open Questions

- Should we expose the catalog via an API/website in addition to GitHub? (Out of scope initially but worth tracking.)
- Should the core components Python package be included in the Kubeflow SDK directly?

## Design Details

### Implementation Phases

1. **Bootstrap:** Create repository, scaffolding, metadata schema, CI pipeline, and packaging structure.
2. **Migration:** Ask community members to move existing core components/pipelines from the main KFP repo, update
   metadata/README templates, and publish initial PyPI release.
3. **Third-Party Onboarding:** Document contribution process for external teams, enable separate packaging workflow.
4. **Automation Rollout:** Deploy maintenance cron jobs (lastVerified sweeps, dependency probes, removal cleanup).
5. **Enhancements:** Add optional catalog website/API after stabilization.

### Rollout and Migration

- Start with a curated set of well-maintained components to validate workflow.
- Keep legacy components in the main repo temporarily with clear deprecation notices; point users to the new repository.
- Remove the legacy components in the KFP repository after 60 days.
- Align the migration timeline with the cleanup work proposed in
  [kubeflow/pipelines#12218](https://github.com/kubeflow/pipelines/discussions/12218) so community messaging and
  automation land consistently across both efforts.

## Risks and Mitigations

- **Fragmentation:** Users might be confused between core and third-party assets.
  - Mitigation: clear packaging split, README disclaimers, AGENTS guidance, and metadata `tier` flag displayed in docs.
- **Maintenance overhead:** Keeping metadata fresh requires effort.
  - Mitigation: automation for reminders/removals, limiting core tier to assets with active owners.
- **Dependency sprawl:** Components may require heavy or conflicting dependencies.
  - Mitigation: dependency probe with opt-out justification, encourage minimal footprints, and categorize optional
    extras.
- **CI costs:** Installing dependencies per PR could be expensive.
  - Mitigation: scope dependency probe to changed assets and allow skip for heavy stacks.
- **Doc generation drift:** Autogenerated README content may become outdated.
  - Mitigation: tie README generation to CI, fail if template is stale or missing.

## Test Plan

### Unit & Compile Checks

- Ensure every component/pipeline compiles to a valid spec using `kfp.compiler` when `compile_check` is enabled.
- Optional pytest suites run for assets with `tests/` directories.
- Dependency probe attempts installation of `packages_to_install` for updated assets unless opt-out is granted.

### Continuous Integration

- PR CI: lint, metadata validation, compile checks, dependency probes, pytest.
- Scheduled CI: weekly verification of `lastVerified` deadlines and dependency probe opt-outs.
- Release CI: packaging workflow tests building wheels and verifying imports.

## Graduation Criteria

To be determined.

## Implementation History

- 2025-10-20: Proposal drafted (this KEP).
- TBD: Repository created.
- TBD: Initial components migrated and first release published.

## Drawbacks

- Additional repository to maintain with separate release cadence.
- Contributors must learn new metadata and documentation requirements.
- Dependency probe may fail for niche dependencies, requiring manual intervention or skip flag justification.

## Alternatives

### Separate Repositories

Require separate repositories for third-party contributions. This would be hard to keep in sync and less user friendly.

### Keep the Existing `components` Directory in the KFP Repo

- Pros: no new repository to maintain; keeps code alongside the rest of KFP.
- Cons: couples release cadence to the full monorepo, makes discoverability harder, keeps inconsistent documentation,
  and discourages external contribution ownership.
