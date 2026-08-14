# Security policy

## Supported versions

The latest released version is supported. Fixes are made on `main` and released from there.

## Reporting a vulnerability

Report privately through
[GitHub's private vulnerability reporting](https://github.com/s4ndr0ne/SqlCdc/security/advisories/new).
Please do not open a public issue for a vulnerability.

Include what the issue is, how to reproduce it, and what an attacker could achieve with it. You
can expect an acknowledgement within a few days.

## Notes for operators

- **Least privilege.** The library needs `SELECT` on the `cdc` schema and the change tables, and
  `SELECT`/`INSERT`/`UPDATE` on its own watermark and dead-letter tables. It does not need
  `db_owner` or `sysadmin` — those are needed only to *enable* CDC, which is a one-off
  administrative step. By default it creates its two state tables on first use; where DDL rights
  at runtime are not acceptable, provision them with `scripts/create-state-tables.sql` and pass
  `createTableIfMissing: false`.
- **Change data is application data.** `CdcChange` carries the before and after images of every
  captured column, and the dead-letter table stores them as JSON. If the source tables hold
  personal or otherwise sensitive data, so do the events, your handlers' logs and
  `dbo.cdc_dead_letter` — treat and retain them accordingly.
- **Connections.** Prefer Entra ID (`Authentication=Active Directory Default` and friends) over
  a password in a connection string, and `Encrypt=True`. `UseAccessTokenCallback` and
  `UseConnectionFactory` exist so credentials never have to sit in configuration.
