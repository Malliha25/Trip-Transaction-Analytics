# CI/CD & DevOps Strategy

## Environments
| Env | Purpose | ADF instance | Databricks workspace | Storage account |
|---|---|---|---|---|
| Dev | Active development, unit tests | `adf-tripanalytics-dev` | `dbw-tripanalytics-dev` | `sttripanalyticsdev` |
| QA | Integration testing, DQ rule validation | `adf-tripanalytics-qa` | `dbw-tripanalytics-qa` | `sttripanalyticsqa` |
| UAT | Business sign-off with production-like volumes | `adf-tripanalytics-uat` | `dbw-tripanalytics-uat` | `sttripanalyticsuat` |
| Prod | Live daily orchestration | `adf-tripanalytics-prod` | `dbw-tripanalytics-prod` | `sttripanalyticsprod` |

## Tooling
- **Azure DevOps** (Boards + Repos + Pipelines) as the single system of record for work
  items, code, and releases.
- **Git integration**: ADF is Git-mode connected (Azure DevOps Repos) to a `collaboration`
  branch (`main`); Databricks notebooks live in **Databricks Repos**, also backed by the
  same Azure DevOps repo, under `/Databricks`.
- **ARM/Bicep templates**: infrastructure (ADF factory, Databricks workspace, storage
  accounts, Key Vault, Logic Apps, RBAC assignments) defined as Bicep modules under
  `/infra`, deployed via `az deployment group create`.

## Branching strategy
```
main            <- protected; always deployable to Prod
 └─ release/*   <- cut from main for a UAT sign-off cycle
 └─ develop     <- integration branch, auto-deploys to Dev
     └─ feature/<ticket-id>-<short-desc>   <- one branch per work item
     └─ bugfix/<ticket-id>-<short-desc>
```
- Feature branches merge into `develop` via PR (min. 1 reviewer, build validation
  required).
- `develop` -> `release/*` promotion triggers the QA pipeline.
- `release/*` -> `main` merge (after UAT sign-off) triggers the Prod release pipeline,
  gated by a manual approval from the Data Platform Lead.

## Deployment process
1. **ADF**: `adf publish` in Git mode generates an ARM template (`ARMTemplateForFactory.json`)
   into the `adf_publish` branch. A DevOps release pipeline consumes this template and
   runs the official `Azure Data Factory Deploy` task per environment, using a
   pre/post-deployment PowerShell script to stop/start triggers safely and clean up
   deleted resources.
2. **Databricks**: notebooks deployed via **Databricks Asset Bundles (DAB)** —
   `databricks bundle deploy -t qa|uat|prod` — which also provisions/updates the Job
   definitions, cluster policies, and permissions declared in `databricks.yml`.
3. **SQL (control/audit schema)**: versioned migration scripts under `/SQL/migrations`,
   applied with a lightweight migration runner (e.g. DbUp or Flyway) as a DevOps
   pipeline task.
4. **Infra (Bicep)**: `az deployment group create -f infra/main.bicep` per environment,
   parameterized via `*.parameters.<env>.json` files.

## Sample Azure DevOps pipeline stages (azure-pipelines.yml, conceptual)
```yaml
stages:
  - stage: Build
    jobs:
      - job: ValidateADF
        steps:
          - script: npm install -g @microsoft/azure-data-factory-utilities
          - script: npm run build validate $(Build.Repository.LocalPath)/ADF adf-tripanalytics-dev
      - job: LintPySpark
        steps:
          - script: pip install flake8 --break-system-packages
          - script: flake8 Databricks/ DataQuality/

  - stage: DeployDev
    dependsOn: Build
    jobs:
      - deployment: DeployADF_Dev
        environment: Dev
        strategy:
          runOnce:
            deploy:
              steps:
                - task: AzureDataFactoryDeploy@2

  - stage: DeployQA
    dependsOn: DeployDev
    jobs:
      - deployment: DeployADF_QA
        environment: QA

  - stage: DeployUAT
    dependsOn: DeployQA
    jobs:
      - deployment: DeployADF_UAT
        environment: UAT

  - stage: DeployProd
    dependsOn: DeployUAT
    jobs:
      - deployment: DeployADF_Prod
        environment: Prod   # environment approval gate configured in DevOps
```

## Configuration management
- Environment-specific values (storage account names, Key Vault URIs, cluster IDs)
  are never hardcoded — ADF uses **Global Parameters** + **Key Vault-backed linked
  service parameters**; Databricks Asset Bundles use `targets.<env>.variables` in
  `databricks.yml`.
- Secrets (SQL passwords, service principal secrets) live exclusively in **Azure Key
  Vault**, referenced by ADF Linked Services and Databricks Secret Scopes — never
  committed to Git.
