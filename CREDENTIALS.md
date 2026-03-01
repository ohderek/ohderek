<div align="center">

<img src="https://capsule-render.vercel.app/api?type=rect&color=0d0d0d&height=160&text=Credential+Management&fontSize=48&fontColor=ff6b35&fontAlignY=52&animation=fadeIn&desc=Environment+Variables+%C2%B7+Key-Pair+Auth+%C2%B7+Secrets+Managers&descSize=18&descAlignY=75&descColor=c9a84c" />

</div>

<br/>

> Every project in this portfolio follows the same rule: **no credentials in code or version control**. Secrets are injected at runtime. The mechanism varies by context — local dev, orchestration, CI/CD, cloud — but the principle is constant.

---

## ◈ Fail-Fast Pattern

Use `os.environ["KEY"]` (bracket notation), not `os.environ.get("KEY")`. The difference matters in production:

```python
# Bad — silent failure
api_key = os.environ.get("CMC_API_KEY")  # returns None if missing
response = client.get(headers={"X-API-KEY": api_key})  # 401, confusing stack trace

# Good — fail immediately with a clear error
api_key = os.environ["CMC_API_KEY"]  # raises KeyError with the variable name
```

A `KeyError` at startup is far easier to diagnose than a `None`-related failure five steps into a pipeline run. Apply this to every required credential.

---

## ◈ Local Development — `.env` + `python-dotenv`

```bash
# .env — never committed (add to .gitignore)
CMC_API_KEY=your-api-key
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=etl_user
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
GITHUB_TOKEN=ghp_xxxxxxxxxxxx
```

```python
from dotenv import load_dotenv
import os

load_dotenv()  # reads .env into os.environ

api_key = os.environ["CMC_API_KEY"]
```

`load_dotenv()` is a no-op when variables are already set in the environment (e.g. in CI or production) — the same code runs unchanged across all contexts.

**Minimum `.gitignore` entries:**

```gitignore
.env
*.p8
*.pem
*.key
profiles.yml          # dbt — contains Snowflake credentials
service-account.json  # GCP service accounts
```

---

## ◈ Snowflake — Key-Pair Authentication

Preferred over password auth across all projects. Passwords appear in query history logs and are subject to rotation policies that break pipelines. RSA key-pair authentication cannot be intercepted in transit and rotates independently of any password policy.

```bash
# Generate RSA key pair (no passphrase — service accounts don't need one)
openssl genrsa -out rsa_key.pem 2048
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub.pem
openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt \
    -in rsa_key.pem -out rsa_key.p8

# Assign the public key to your Snowflake service user
ALTER USER etl_user SET RSA_PUBLIC_KEY='<contents of rsa_key.pub.pem>';

# Verify
DESC USER etl_user;  -- RSA_PUBLIC_KEY_FP should be populated
```

The **private key file** (`rsa_key.p8`) stays on disk. Reference its path via an env var — never its contents:

```bash
SNOWFLAKE_PRIVATE_KEY_PATH=/secrets/rsa_key.p8   # path only
```

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
)
```

---

## ◈ Apache Airflow — Connections and Variables

Airflow encrypts both at rest using the `AIRFLOW__CORE__FERNET_KEY`. Neither appears in DAG code or logs.

```bash
# Service credentials via Connections
airflow connections add snowflake_default \
    --conn-type    snowflake \
    --conn-host    your-account.snowflakecomputing.com \
    --conn-login   etl_user \
    --conn-extra   '{"private_key_path": "/secrets/rsa_key.p8", "role": "TRANSFORMER"}'

# API keys via Variables
airflow variables set INCIDENT_IO_API_KEY  "your-key"
airflow variables set GITHUB_TOKEN         "ghp_xxxxxxxxxxxx"
airflow variables set AI_GATEWAY_API_KEY   "your-key"
```

```python
# In DAG code — never hardcoded
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

api_key  = Variable.get("INCIDENT_IO_API_KEY")
sf_hook  = SnowflakeHook(snowflake_conn_id="snowflake_default")
```

For secrets that should never appear in the Airflow UI at all, use a [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) (AWS SSM, GCP Secret Manager, HashiCorp Vault) — Airflow fetches the value at task execution time without storing it locally.

---

## ◈ Prefect — Secret Blocks

Prefect Secret Blocks encrypt the value server-side. The value is never logged, never serialised into flow run state, and never visible in the Prefect UI after creation.

```python
# Store once (run interactively or in a setup script)
import asyncio
from prefect.blocks.system import Secret

asyncio.run(Secret(value="ghp_xxxxxxxxxxxx").save("github-token"))
asyncio.run(Secret(value="your-snowflake-key-contents").save("snowflake-private-key"))
```

```python
# Reference in flow — value fetched at runtime, never logged
@flow
async def ingest_pull_requests():
    github_token = await Secret.load("github-token")
    # github_token.get() returns the plaintext value
```

For GCS/S3 credentials, use the dedicated Prefect GCS or S3 Blocks rather than raw Secret Blocks — they handle auth configuration more completely.

---

## ◈ dbt — `env_var()` in profiles.yml

`profiles.yml` is never committed. It reads credentials from the environment at dbt invocation time.

```yaml
# ~/.dbt/profiles.yml  (outside the project directory, never in git)
analytics:
  target: dev
  outputs:
    dev:
      type:              snowflake
      account:           "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:              "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path:  "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role:              "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}"
      warehouse:         "{{ env_var('SNOWFLAKE_WAREHOUSE', 'ADHOC__MEDIUM') }}"
      database:          ANALYTICS
      schema:            "sandbox_{{ env_var('DBT_USER', 'dev') }}"
      threads:           4
```

The `env_var('KEY', 'default')` two-argument form provides a fallback — useful for optional config like role and warehouse without breaking the profile when they're not set.

---

## ◈ Databricks — PATs and Service Principals

```bash
# Personal Access Token — local dev and notebooks only
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# CLI profile (alternative to env vars for local use)
databricks configure --token
# Host: https://your-workspace.azuredatabricks.net
# Token: dapi...
```

For production (CI/CD, Asset Bundles), use a **Service Principal** with OAuth M2M instead of a PAT. PATs are tied to a personal user account and expire; service principals are workspace-scoped and survive user offboarding.

```bash
# Azure AD service principal for Databricks
export ARM_CLIENT_ID="your-app-id"
export ARM_CLIENT_SECRET="your-client-secret"
export ARM_TENANT_ID="your-tenant-id"
```

```yaml
# databricks.yml (Asset Bundle) — references env vars, never inline secrets
bundle:
  name: taxi_lakehouse

workspace:
  host: ${workspace_host}    # set in environment or CI secret

variables:
  workspace_host:
    default: ${DATABRICKS_HOST}
```

---

## ◈ GitHub Actions — Repository Secrets

Secrets stored under **Settings → Secrets and variables → Actions** are masked in all log output — even if accidentally `echo`-ed in a run step.

```yaml
# .github/workflows/etl.yml
jobs:
  run-pipeline:
    env:
      CMC_API_KEY:                ${{ secrets.CMC_API_KEY }}
      SNOWFLAKE_ACCOUNT:          ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER:             ${{ secrets.SNOWFLAKE_USER }}
      # Store private key contents (not path) in Actions — no persistent disk
      SNOWFLAKE_PRIVATE_KEY_B64:  ${{ secrets.SNOWFLAKE_PRIVATE_KEY_B64 }}
    steps:
      - name: Decode private key
        run: |
          echo "$SNOWFLAKE_PRIVATE_KEY_B64" | base64 -d > /tmp/rsa_key.p8
          echo "SNOWFLAKE_PRIVATE_KEY_PATH=/tmp/rsa_key.p8" >> $GITHUB_ENV
```

Store the private key as base64 in the secret (no newline issues), decode it to a temp file at the start of the job, then point `SNOWFLAKE_PRIVATE_KEY_PATH` at it.

For multi-environment deployments (dev/staging/prod), use **Environment Secrets** scoped to specific deployment environments rather than repository-level secrets — this prevents a dev workflow from accessing production credentials.

---

## ◈ Cloud Secrets Managers

For production deployments, a dedicated secrets manager eliminates the env var surface entirely. The application fetches credentials at runtime from the manager — they never appear in process environment or shell history.

**AWS Secrets Manager:**

```python
import boto3, json

def get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

creds = get_secret("prod/crypto-market-data/snowflake")
conn = snowflake.connector.connect(
    account=creds["account"],
    user=creds["user"],
    private_key_file_pwd=creds["private_key_passphrase"],
)
```

**GCP Secret Manager:**

```python
from google.cloud import secretmanager

def get_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("UTF-8")

api_key = get_secret("my-gcp-project", "cmc-api-key")
```

**AWS Systems Manager Parameter Store** (for simpler string values):

```python
import boto3

ssm = boto3.client("ssm", region_name="us-east-1")
api_key = ssm.get_parameter(
    Name="/prod/crypto-market-data/cmc_api_key",
    WithDecryption=True,
)["Parameter"]["Value"]
```

---

<div align="center">

<a href="https://www.linkedin.com/in/derek-o-halloran/">
  <img src="https://img.shields.io/badge/LINKEDIN-0d0d0d?style=for-the-badge&logo=linkedin&logoColor=ff6b35" />
</a>&nbsp;
<a href="https://github.com/ohderek/data-engineering-portfolio">
  <img src="https://img.shields.io/badge/DATA_PORTFOLIO-0d0d0d?style=for-the-badge&logo=github&logoColor=ff6b35" />
</a>

<br/><br/>

<img src="https://capsule-render.vercel.app/api?type=waving&color=0,0d0d0d,100,1a1a2e&height=100&section=footer" />

</div>
