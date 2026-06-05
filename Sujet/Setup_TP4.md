# Setup - Lab 4 (Big Data Processing - EPF)

> **Read and execute this document BEFORE Lab.**
>
> Lab 4 adds **PySpark inside Docker**.

---

## 1. Prerequisites

| Requirement | How to check |
|---|---|
| 64-bit OS | Windows 10/11, macOS 12+, or Linux |
| **8 GB RAM** minimum | Task Manager / Activity Monitor |
| **12 GB free disk** | File Explorer / `df -h` |
| Admin rights | Required to install Docker |

---

## 2. Install Docker Desktop

If Docker is already installed, skip to §3.

### Windows

1. Download [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Enable **WSL 2** when prompted; restart if asked.
3. **Settings → Resources → Advanced**: CPUs ≥ 2, Memory ≥ **4 GB**.
4. Apply & Restart.

### macOS

Install Docker Desktop (Apple Silicon or Intel). Same resource settings as above.

### Linux

Install Docker Engine + Compose plugin; add your user to the `docker` group.

---

## 3. Verify Docker

```bash
docker --version
docker compose version
docker run --rm hello-world
```

---

## 4. Get the lab files (Moodle)

The **subject and starter kit are only on Moodle** (ZIP).

1. Download `lab4_student.zip` and `lab4.pdf` from Moodle.
2. Extract to a **short path without spaces**, e.g. `C:\dev\lab4` or `~/lab4`.
3. Create `.env`:
   - Linux/macOS: `cp .env.example .env` then set `AIRFLOW_UID=$(id -u)` in the file (numeric only).
   - Windows: `copy .env.example .env` and keep `AIRFLOW_UID=50000`.

> **Windows + OneDrive:** use a junction if needed:  
> `mklink /J C:\dev\lab4 "C:\Users\you\OneDrive\...\lab4_student"`

---

## 5. Build the Lab 4 image

The image is **built locally** (not pulled). It installs DuckDB, PySpark, and OpenJDK.

```bash
cd lab4_student
docker compose pull postgres redis
docker compose build airflow-init
```

If you see `No module named 'pyspark'` or Java errors, rebuild:

```bash
docker compose build --no-cache airflow-init
docker compose up -d --force-recreate
```

---

## 6. Initialize and start Airflow

```bash

docker compose up airflow-init
docker compose up -d
```

Wait 60–90 seconds, then:

```bash
docker compose ps
```

All services should be `Up` or `healthy`.

---

## 7. Verify Java and PySpark

```bash
docker compose exec airflow-worker java -version
docker compose exec airflow-worker python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print('Spark', s.version); s.stop()"
```

Both commands must succeed without error.

---

## 8. Smoke test

From `lab4_student/` on the **host** (stack must be `up`, image built in §5):

```bash
python scripts/smoke_test.py
```

This seeds vendor data, unpauses **`lab4_starter`**, triggers **`2026-06-01`**, and waits for `data/reports/dashboard_2026-06-01.json`.

**Manual check (optional):** open <http://localhost:8080> (`airflow` / `airflow`) and confirm the four tasks are green.

> The Spark task in `lab4_starter` only checks that PySpark runs (`spark.range(1).count()`). Your capstone implements real KPIs in `include/team_<name>_spark.py`.

---

## 9. Stop / restart

```bash
docker compose stop      # keep data
docker compose start     # lab day
docker compose down -v   # full reset (deletes DB)
```

---

## 10. Common pitfalls

| Symptom | Fix |
|---|---|
| Port 8080 in use | Set `AIRFLOW_UI_PORT=8081` in `.env`, recreate containers |
| `Java not found` in Spark task | Rebuild image; check `docker compose exec airflow-worker java -version` |
| FileSensor never succeeds | Run `vendor_drop` for that `ds` date first |
| DAG import error | `docker compose logs airflow-scheduler \| tail -50` |
| Linux permission errors on logs | Fix `AIRFLOW_UID` in `.env` |
