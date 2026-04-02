# Environment Variables

Reference generated from `.env.example`. Copy `.env.example` to `.env` and fill in the values.
The `.env` file must **never** be committed to git.

<!-- AUTO-GENERATED:START -->

## SAP GUI Credentials

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `SAP_USERNAME` | Yes | SAP logon username. Also accepts `SAP_USER` | — |
| `SAP_PASSWORD` | Yes | SAP logon password. Also accepts `SAP_PASS` | — |
| `SAP_CLIENT` | No | SAP client number (e.g. `100`) | — |
| `SAP_LANGUAGE` | No | SAP logon language | `PT` |
| `SAP_POST_LOGIN_SLEEP_SECONDS` | No | Delay after SAP login before automation starts | `0` |

## Control Plane

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `DATABASE_URL` | Yes | PostgreSQL connection string (SQLAlchemy + psycopg) | `postgresql+psycopg://sap_automation:sap_automation@localhost:5432/sap_automation` |
| `REDIS_URL` | No | Redis URL for job queuing | `redis://localhost:6379/0` |
| `SAP_OUTPUT_ROOT` | No | Root directory for run artifacts | `output` |
| `SAP_QUEUE_NAME` | No | Redis queue name for job dispatch | `sap-default` |
| `SAP_SCHEDULER_POLL_SECONDS` | No | Interval for scheduler to poll for due schedules | `15` |
| `SAP_RUNNER_POLL_SECONDS` | No | Interval for runner to poll for queued jobs | `5` |
| `SAP_RUNNER_HEARTBEAT_SECONDS` | No | Interval for runner heartbeat updates | `15` |
| `SAP_RUNNER_ID` | No | Unique identifier for this runner instance | — |

<!-- AUTO-GENERATED:END -->
