# Changelog

## 2.3.1 — SES-only stabilization (structure preserved)
- Disable ARIMA & HWES; run **SES-M** and **SES-Q** only.
- Durable job files: `backend/data/output/_jobs/<job_id>.json`.
- Verbose progress messages (`SES-M i/N`, `SES-Q i/N`).
- U.S. state filter optional; accepts `state` or `state_name`.
- CSV columns: `DATE, VALUE, SES-M, SES-Q`; filename `[TARGET_VALUE]_[STATE]_[COUNTY]_[CITY]_[CBSA]_[TYPE].csv`.


## 2.0.5 — Verbose progress + durable jobs
- Add fine-grained progress messages: which model (ARIMA/SES/HWES) and which period (monthly or quarterly window) is running.
- Keep jobs **durable** on disk; status shows `"paused"` when a heartbeat is stale and can be **resumed**.
- New endpoint: `POST /classical/resume?job_id=...` to continue long jobs after Render sleep.
- U.S. state filter remains **optional**; both `state` and `state_name` accepted and coalesced.
- CSV unchanged (spec): `DATE, VALUE, ARIMA-M, ARIMA-Q, SES-M, SES-Q, HWES-M, HWES-Q`; filename `[TARGET_VALUE]_[STATE]_[COUNTY]_[CITY]_[CBSA]_[TYPE].csv`.


## 2.0.4 — Durable jobs, optional state filter, resume support
- Persist job metadata to disk (`data/output/_jobs/<job_id>.json`) so status/download work even if the Render dyno sleeps or requests land on a different instance.
- Add **heartbeat** + **auto-pause** detection: if a running job’s heartbeat is stale, status returns `"state":"paused"` with the last known percent.
- Add `POST /classical/resume?job_id=...` to restart a paused/missing worker.
- Keep progress contract the same (`state`, `message`, `percent`, `done`, `total`).
- Make the **U.S. state filter optional** everywhere; accept both `state` and `state_name`.
- CSV: unchanged (spec): columns `DATE, VALUE, ARIMA-M, ARIMA-Q, SES-M, SES-Q, HWES-M, HWES-Q`; filename `[TARGET_VALUE]_[STATE]_[COUNTY]_[CITY]_[CBSA]_[TYPE].csv`.


## 2.0.3 – Fix probe/start param handling
- Make `state` **optional** and accept both `state` and `state_name` for `/classical/probe` and `/classical/start`.
- `/classical/start` now accepts **JSON body** or **query params** (both supported).
- Progress reporting unchanged (`state`, `message`, `percent`, `done`, `total`).
- CSV naming unchanged (drops `F_` on output): `[TARGET_VALUE]_[STATE]_[COUNTY]_[CITY]_[CBSA]_[TYPE].csv`.
- CSV columns unchanged (exactly 8 columns): `DATE, VALUE, ARIMA-M, ARIMA-Q, SES-M, SES-Q, HWES-M, HWES-Q`.

## 2.0.2
- (previous) Added acceptance of `state` alongside `state_name`.

## 2.0.1 / 2.0.0
- (previous) Hardening, CORS, and classical-only flow.



## v1.2.3 — 2025-09-06
- Background jobs for Classical forecasts:
  - `POST /classical/start` → returns `job_id`
  - `GET /classical/status?job_id=…` → live progress (`state/done/total/message`)
  - `GET /classical/download?job_id=…` → CSV when ready
- Keeps existing `/classical/probe` and `/classical/export_*` endpoints.
