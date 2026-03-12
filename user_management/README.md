# ArcGIS Enterprise User Management

## pre-requisites

* python instance with latest `age-oauth` installed
* a dedicated OAuth 2.0 client created inside the desired ArcGIS Enterprise
* onboard the aforementioned client to `age-oauth`

## setup

* fill out `policy.json`
* fill out `smtp.json`

## run the latency guard

Report only (no changes, no email notifications):

```
python user_latency_guard.py --policy policy.json --report
```

Dry run (no changes, no email notifications - show actions that would be taken):

```
python user_latency_guard.py --policy policy.json --dry-run
```

### Live ops (perform changes, send out email notifications):

Can do this a few ways.

First, one-shot:

```powershell
$env:SMTP_PASSWORD="your_smtp_password"
python user_latency_guard.py --policy policy.json
```

Or create a PS wrapper like `user_latency_guard.ps1` with the following contents:

```powershell
$env:SMTP_PASSWORD = "your_smtp_password"
conda activate env_name
python C:\path\to\user_latency_guard.py --policy C:\path\to\policy.json
```




## query inactive users

Dump all demotions:

```
python query_inactive_users.py dump
```

Dump all tracked users (not yet demoted):

```
python query_inactive_users.py --table tracked_users dump
```

Look up a demotion record by username:

```
python query_inactive_users.py user --username jdoe
```

Look up a demotion record by email:

```
python query_inactive_users.py email --email jdoe@somewhere.com
```

Query demotions by date range (full days):

```
python query_inactive_users.py range --start 2026-02-01 --stop 2026-02-29
```

Same thing but export it to CSV

```
python query_inactive_users.py --csv range --start 2026-02-01 --stop 2026-02-29
```
