# Spark Optimization

## OPTIMIZE , ZORDER BY and VACUUM

>### Q: What is OPTIMIZE and VACUUM command in Databricks, its differences and when to use which?

The OPTIMIZE command in Databricks _merges many small files into larger, better-sized files_ to make queries run much faster. When you write data to a Delta table often, it creates many tiny files. This slows down read operations because your compute engine spends too much time opening files instead of reading data. OPTIMIZE fixes this by packing those small files together.

The OPTIMIZE command uses a process called **bin-packing**. It combines small Parquet data files into larger files (around 1 GB by default). You can also _sort the data_ inside these files by specific columns _using the ZORDER BY clause to skip irrelevant data during queries_.

```sql
-- Compact small files in a table
OPTIMIZE sales_table;

-- Compact and sort data by a specific column for faster lookups
OPTIMIZE sales_table ZORDER BY (customer_id);

```

The VACUUM command in Databricks **permanently removes** _obsolete, unreferenced data files (such as old Parquet files)_ from a Delta table directory to _optimize cloud storage costs and ensure data compliance_. When we run UPDATE, DELETE, or OPTIMIZE commands, Delta Lake doesn't immediately delete the physical files; instead, it _logically removes them from the transaction log_. VACUUM cleans up these residual, stale files.

**You can run VACUUM using SQL directly in a Databricks Notebook or SQL warehouse:**
```sql
-- 1. Safe Check: See what would be deleted without actually deleting it
VACUUM sales_table DRY RUN;

-- 2. Clean up using the default 7-day retention threshold
VACUUM sales_table;

-- 3. Clean up and retain historical data for a specific duration (e.g., 100 hours)
VACUUM sales_table RETAIN 100 HOURS;

```
**Databricks supports two execution modes for vacuuming Delta tables:**

| Mode | How it Works | Best Used For |
|---|---|---|
| FULL (Default) | Scans both the transaction log and the physical cloud storage directory to find all unreferenced files. | Scenarios with potential orphaned files or untracked directory items. Slower for large tables. |
| LITE | Relies strictly on the Delta transaction log to pinpoint stale files, avoiding a heavy storage directory scan. | Large tables that require fast, frequent cleanups. |

```sql
-- Explicitly running in Lite mode
VACUUM sales_table LITE;
```
#### Important Rules & Caveats:

* The 7-Day Default: By default, VACUUM only removes unreferenced files older than 7 days (168 hours).
* Time Travel Impact: Once files are permanently removed by VACUUM, you lose the ability to time travel or restore the table back to a version older than your chosen retention window.
* Safety Check Bypass: Delta Lake blocks you from setting a retention threshold of 0 HOURS by default to prevent accidental data loss or breaking concurrent queries. If you absolutely need to bypass this safety check, switch off the safety flag in your Spark session configuration:
    ```sql
    SET spark.databricks.delta.retentionDurationCheck.enabled = false;
    ```
* Predictive Optimization: If you use Unity Catalog and have predictive optimization enabled, Databricks will automatically trigger VACUUM tasks on your behalf, reducing the need for manual scheduling.

#### Differences Between VACUUM and OPTIMIZE

| Feature | OPTIMIZE | VACUUM |
|---|---|---|
| Main Goal | Improve query performance and speed. | Reduce storage costs and clean up data. |
| Action on Files | Combines small files into larger, optimized files. | Permanently deletes old, unreferenced files. |
| Data History | Keeps all active and historical data safe and intact. | Destroys history older than the retention limit. |
| Time Travel | Does not affect time travel capabilities. | Breaks time travel past the retention window. |
| Safety Risk | Low risk; safe to run frequently. | High risk if retention is set too short (0 hours). |

#### How They Work Together

* Use OPTIMIZE regularly (daily or weekly) to keep your tables fast for users and queries.
* Use VACUUM periodically (like the default 7-day window) to delete the old files left behind after updates, deletes, and optimizations.

---

>### Q: How to handle multi-cluster setups for optimization job (created manually), prevent timeout or error throw in case of very large table, and configure Predictive Optimization?

#### 1. Multi-Cluster Architecture (Saving Up to 80% on Costs)
Running maintenance on massive tables can be expensive. Do not use your primary interactive or expensive multi-node worker clusters for this. Instead, configure your Databricks Workflow to use a dedicated cluster pool with these configurations:

* Use Spot Instances: OPTIMIZE and VACUUM are fault-tolerant batch operations. Using Spot Instances for worker nodes will slash your compute costs by up to 80%.
* Select Compute-Optimized (F-Series / C-Series) or Memory-Optimized (R-Series): OPTIMIZE is highly CPU and memory intensive because it reads, merges, and rewrites huge volumes of data.
* Enable Single-Node or Minimal Workers: If you are running VACUUM LITE, the processing happens mostly on the driver node via log file checks. You don't need a massive multi-node cluster just to delete files.

#### 2. Handling Very Large Tables (Preventing OOMs and Timeouts)
If your tables are in the multi-terabyte range, a basic sequential loop will likely crash with an Out of Memory (OOM) error or hit a timeout limit. Implement these guardrails:

* Isolate Partition Maintenance: Instead of optimizing the whole table at once, optimize it incrementally by filtering for specific partitions (e.g., just the last 30 days of data).

`OPTIMIZE sales_table WHERE order_date >= '2026-09-01';`

* Leverage VACUUM LITE: For massive scales, scanning cloud storage buckets directly can take hours or even days. Switching to LITE instructs Databricks to strictly follow the transaction logs, finishing the task in minutes.

`VACUUM sales_table LITE;`

* Adjust Shuffle Partitions: Increase the Spark shuffle partitions prior to running the optimization task to give Spark more granular control over its memory management:

`SET spark.sql.shuffle.partitions = 2000;`

#### 3. Turning on Predictive Optimization (Zero-Management Setup)
If you want to completely skip managing clusters, scripts, or workflows altogether, you can let Databricks handle it under the hood. As long as your tables are managed by Unity Catalog, executing this single command will pass the operational overhead over to Databricks:

Enable it across the entire catalog (all schemas and tables)

`ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;`

- **How it evaluates your tables:**

   1. Intelligence Check: Databricks evaluates your table's write patterns, file size distribution, and query patterns.
   2. Serverless Execution: It spins up a dedicated serverless compute pool in the background specifically when your system is at rest.
   3. No Downtime: It completes the OPTIMIZE and VACUUM procedures quietly without ever locking your operational user queries or data pipelines.




