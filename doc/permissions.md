# Permissions
Spark job server implements a basic authorization management system to control access to single resources. By default,
users always have access to all resources (`ALLOW_ALL`). Authorization is implemented by checking the *permissions* of a
user with the required permissions of an endpoint.

| Name            | Identifier      | Routes                                                                                                                                                                 |
|-----------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| ALLOW_ALL       | *               |                                                                                                                                                                        |
| BINARIES        | binaries        | `GET /binaries` <br />`GET /binaries/<appName>` <br /> `POST /binaries/<appName>` <br /> `DELETE /binaries/<appName>`                                                  |
| BINARIES_READ   | binaries:read   | `GET /binaries` <br /> `GET /binaries/<appName>`                                                                                                                       |
| BINARIES_UPLOAD | binaries:upload | `POST /binaries/<appName>`                                                                                                                                             |
| BINARIES_DELETE | binaries:delete | `DELETE /binaries/<appName>`                                                                                                                                           |
| CONTEXTS        | contexts        | `GET /contexts` <br /> `GET /contexts/<contextName>` <br /> `POST /contexts/<contextName>` <br /> `DELETE /contexts/<contextName>` <br /> `PUT /contexts?reset=reboot` |
| CONTEXTS_READ   | contexts:read   | `GET /contexts` <br /> `GET /contexts/<contextName>`                                                                                                                   |
| CONTEXTS_START  | contexts:start  | `POST /contexts/<contextName>`                                                                                                                                         |
| CONTEXTS_DELETE | contexts:delete | `DELETE /contexts/<contextName>`                                                                                                                                       |
| CONTEXTS_RESET  | contexts:reset  | `PUT /contexts?reset=reboot`                                                                                                                                           |
| DATA            | data            | `GET /data` <br /> `DELETE /data/<filename>` <br /> `POST /data/<filename>` <br /> `PUT /contexts?reset=reboot`                                                        |
| DATA_READ       | data:read       | `GET /data`                                                                                                                                                            |
| DATA_UPLOAD     | data:upload     | `POST /data/<filename>`                                                                                                                                                |
| DATA_DELETE     | data:delete     | `DELETE /data/<filename>`                                                                                                                                              |
| DATA_RESET      | data:reset      | `PUT /data?reset=reboot`                                                                                                                                               |
| JOBS            | jobs            | `GET /jobs` <br /> `GET /jobs/<jobId>` <br />  `GET /jobs/<jobId>/config` <br /> `DELETE /jobs/<jobId>`                                                                |
| JOBS_READ       | jobs:read       | `GET /jobs` <br /> `GET /jobs/<jobId>` <br /> `GET /jobs/<jobId>/config`                                                                                               |
| JOBS_START      | jobs:start      | `POST /jobs`                                                                                                                                                           |
| JOBS_DELETE     | jobs:delete     | `DELETE /jobs/<jobId>`                                                                                                                                                 |

Additionally, permissions can be hierarchically stacked. The `BINARIES` permission includes the `BINARIES_READ`,
`BINARIES_UPLOAD` and `BINARIES_DELETE`. Similarly, `CONTEXTS`, `DATA` and `JOBS` aggregate multiple permissions.

## Unprotected Routes
| Routes                        | Comment                                                                                                                                               |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------
| `GET /healthz`                | Access to health routes is not restricted.                                                                                                            |
| `GET /` <br /> `GET /html/*`  | Access to jobserver UI is not restricted. Instead, the calls to load actual data displayed in the UI are affected by the permissions mentioned above. |
