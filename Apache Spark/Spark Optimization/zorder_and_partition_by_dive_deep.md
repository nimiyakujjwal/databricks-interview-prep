# ZORDER BY & PARTITION BY (deep dive)

>## Q: What is ZORDER BY, how it works, when to use and when not?
The ZORDER BY clause is a **data layout optimization technique** used alongside the OPTIMIZE command. It reorganizes your data by **co-locating related information within the same physical files**.

It uses a mathematical concept called a _Z-order curve_ to map multi-dimensional data (multiple columns) into a single dimension while preserving spatial locality.

## How it Works (The Short Version)

When you query a standard table, Spark often has to read every single file to find the rows you asked for.
When you run OPTIMIZE ... ZORDER BY (column_name), **Databricks clusters rows with similar column values into the same Parquet files**. **It then records the minimum and maximum values of those columns for each file in the Delta log (Data Skipping metadata)**. When a query runs, Spark looks at the metadata, realizes the data it needs isn't in File A or File B, and skips reading them entirely. This results in massive query speedups.

## When to Use ZORDER BY
You should use ZORDER BY when a table meets the following criteria:

* High Cardinality Columns: The column is frequently used in query filters (e.g., WHERE customer_id = 12345) and has a large number of unique values.
* Frequent Read Queries: The table is queried often by business intelligence tools, dashboards, or downstream pipelines.
* Large Data Volume: The table is large enough (hundreds of gigabytes or terabytes) that file-skipping actually provides a noticeable performance gain.

**Good Candidate Examples:**

* customer_id, user_id, or device_id (high cardinality, frequent lookups).
* product_sku or store_id (frequently used in WHERE and JOIN conditions).

## When NOT to Use ZORDER BY

* Low Cardinality Columns: Do not Z-Order on columns like gender, status_code, or boolean flags (True/False). Because these values repeat constantly everywhere, they will end up spread across every single file anyway, making the optimization useless.
* Too Many Columns: **Limit your Z-Order to 1 to 4 columns max**. The efficiency of the Z-order curve drops drastically with every column you add (a phenomenon known as the _curse of dimensionality_).
* Highly Partitioned Columns: If a table is already physically partitioned by a column (e.g., PARTITIONED BY (year, month)), do not include those same columns in your ZORDER BY. It is redundant and wastes compute power.
* Small Tables: If a table is only a few megabytes or gigabytes, standard OPTIMIZE without Z-Ordering is more than enough.

## Syntax Example
For Terabyte-scale delta table:
```sql
-- Optimizing a table by clustering customer and product data together
OPTIMIZE main.prod.huge_sales_table 
ZORDER BY (customer_id, product_sku);
```

---
>## Q: How does ZORDER BY works algorithmically at low-level?
At a low level, ZORDER BY works by **mapping multi-dimensional data into a single dimension using a mathematical space-filling curve called the Z-order curve (also known as the Morton space-filling curve)**.

Here is the exact step-by-step programmatic and algorithmic breakdown of what Databricks does under the hood when you run a ZORDER BY command on a Terabyte-scale table.

### Step 1: Interleaving Bits (The Core Math)
When you tell Databricks to ZORDER BY (Age, Salary), it takes those two columns and transforms their values into binary representations. It then interleaves their bits to create a single, unified numeric key (the Morton code).

Imagine we have two attributes converted to 3-bit binary numbers:

* Age (X): 3 → 0 1 1
* Salary (Y): 6 → 1 1 0

To calculate the Z-order key, Spark mixes the bits of X and Y alternately, starting from the most significant bit (left to right):

`{Bit 1 of Y} → {Bit 1 of X} → {Bit 2 of Y} → {Bit 2 of X} ...`
```text
Y bits:   1       1       0
           \     / \     / \
X bits:     0       1       1

            | |     | |     | |
Z-Key:    1 0     1 1     0 1  => Binary: 101101 (Decimal: 45)
```
By doing this for every row, multi-dimensional coordinates are mapped sequentially along a "Z" shaped path. **Data points that are close to each other in multi-dimensional space end up with Z-keys that are numerically very close**.

## Step 2: Sorting and Bin-Packing
Once Spark calculates this internal Z-key for every row in the target dataset, it executes a standard global sort operation based strictly on that single Z-key.

   1. Global Sort: Spark _shuffles and sorts_ the rows sequentially by their Z-keys.
   2. Bin-Packing: It _groups_ these sorted rows and packs them into brand new physical Parquet files (targeting the standard ~1GB size).

Because the rows are sorted by Z-keys, rows containing similar values for all Z-ordered columns are tightly packed into the exact same Parquet files.

## Step 3: Generating Data-Skipping Metadata
After writing the physical Parquet files, Databricks reads the files one last time to extract metadata for the Delta Lake Transaction Log (_delta_log/).

For every single Parquet file generated, it computes and saves:

* The _Minimum value_ of the Z-ordered columns in that file.
* The _Maximum value_ of the Z-ordered columns in that file.
```json
{
  "add": {
    "path": "part-00000.parquet",
    "stats": "{\"numRecords\":1000000,\"minValues\":{\"Age\":25,\"Salary\":50000},\"maxValues\":{\"Age\":30,\"Salary\":65000}}"
  }
}
```

## Step 4: The Payoff – Low-Level Query Execution
When a user executes a query with a filter, Spark's _Catalyst Optimizer_ leverages this low-level structure to completely bypass reading the actual files.
```sql
SELECT * FROM users WHERE Age = 28 AND Salary = 90000;
```

   1. **Metadata Scan**: Instead of opening massive Terabyte-sized Parquet data files, Spark opens the tiny JSON/parquet files inside the `_delta_log/` directory first.
   2. **Range Evaluation**: It checks your query constants (28 and 90000) against the minValues and maxValues arrays of every file block.
   3. **File Dropping**: For "part-00000.parquet" (Min Age: 25, Max Age: 30 | Min Salary: 50k, Max Salary: 65k), Spark instantly notices that 90000 falls outside the salary range.
   4. **I/O Avoidance**: Spark _drops the pointer_ to "part-00000.parquet" completely. **The cluster storage engine never requests that file from Azure ADLS Gen2, slashing network overhead and disk I/O to near zero**.

---

## Q: What is PARTITION BY, when to use, when not and how it compares with ZORDER BY?

PARTITION BY is a **physical data layout technique** that **separates your data into distinct subdirectories (folders) on cloud storage (like Azure ADLS Gen2) based on the values of one or more specific columns**.
For example, if you partition a table by year, Databricks physically writes your data into separate folders like year=2025/ and year=2026/. When a user runs a query with WHERE year = 2026, Spark completely ignores all other folders, which is a process called partition pruning.

## When to Use PARTITION BY (And When Not)
### 🟢 When to Use It

* **Massive Tables**: Only partition tables that are _1 Terabyte or larger_.
* **Low Cardinality Columns**: The column must have a small, predictable number of unique values (e.g., year, month, region, or business_unit).
* **Consistent Query Filters**: The partition column must be used in almost every single query filter or downstream JOIN operation.
* **File Size Balance**: _Each partition folder should contain at least 1 Gigabyte of data_. If your partitions only contain a few Megabytes, you will create a "small file problem" that slows down your system.

### 🔴 When NOT to Use It

* **Tables Under 1 TB**: For small or medium tables, partitioning adds unnecessary metadata management overhead and actually hurts performance. Databricks officially recommends keeping tables unpartitioned unless they are very large.
* **High Cardinality Columns**: Never partition by columns with millions of unique values (like timestamp, user_id, or transaction_id). This creates millions of tiny folders, crashing your storage performance.

## Direct Comparison: PARTITION BY vs. ZORDER BY

| Feature | PARTITION BY | ZORDER BY |
|---|---|---|
| Storage Structure | Physically splits data into separate folders/directories. | Organizes data internally within the files themselves. |
| Column Choice | Low Cardinality (Few unique values, like region). | High Cardinality (Many unique values, like customer_id). |
| When it Happens | Dictated when the table is created (and applied during writes). | Applied later during an active OPTIMIZE routine. |
| Max Columns | 1 to 2 columns max. | 1 to 4 columns max. |

## Should Both Be Used Together?
Yes, but **only for very large tables** (multi-TB scale) and they **must be used on entirely _different_ columns**.

They solve two different problems: 
- PARTITION BY handles macro-level filtering (finding the right folder), while 
- ZORDER BY handles micro-level filtering (finding the right file blocks inside that folder).

## Why you must never use them on the same column:
If you partition a table by year, the data is already perfectly separated into year-based folders. Running a ZORDER BY (year) inside those folders is a total waste of compute power because every single record in that folder already has the exact same year.

## The Best Scenario/ use-case to Combine Them
### The Ideal Scenario
You have a 5 Terabyte logging table that stores global user events.

* [PARTITION BY] Users always filter their queries by a specific date range (e.g., checking last week's logs).
* [ZORDER BY] Users frequently look up specific users (user_id) or specific status codes (status) within those dates.

### The Best Way to Implement It

Step 1: Create the table with macro-level partitioning
```sql
CREATE TABLE main.prod.user_events (
    event_id STRING,
    user_id STRING,
    event_date DATE,
    status STRING,
    payload STRING
)USING DELTAPARTITION BY (event_date); -- Low cardinality, used in every query
```
Step 2: Run micro-level optimization periodically

When running your maintenance job, you target the files inside those partition folders to sort them by your high-cardinality search keys.
```sql
-- This organizes the files inside the date folders by user_id
OPTIMIZE main.prod.user_events 
ZORDER BY (user_id);
```
   
---

>## Q: How does PARTITION BY works algorithmically/ programmatically under the hood?

At a low level, PARTITION BY works by completely altering how the storage engine organizes physical data on disk. Instead of writing all data into a unified set of files, it uses an algorithmic approach based on Directory-based Data Hashing and Pruning.

Below is the exact programmatic and algorithmic breakdown of what Databricks and Spark do under the hood during writes and reads when a table is partitioned.

### Step 1: The Write Phase – Dynamic Partition Distribution
When you execute an INSERT or COPY INTO command on a partitioned table, Spark’s execution engine doesn't just stream rows to disk. It introduces an internal transformation step in the physical execution plan called FileFormatWriter.
```sql
-- Example: Inserting data into a table partitioned by (event_date)
INSERT INTO main.prod.user_events SELECT * FROM staging_data;
```
#### The Algorithmic Flow:

   1. **Row Projection**: As rows flow through the Spark cluster's executor nodes, Spark evaluates the value of the partition column (e.g., event_date) for every single row.
   2. **Bucket/Folder Hashing**: Spark generates a string representation of that value formatted as a standard URI path segment (e.g., event_date=2026-10-03).
   3. **Data Routing (Shuffling)**: To prevent multiple CPU cores from constantly fighting to open and write to the same storage folders simultaneously, Spark performs a local or global sort/shuffle. It clusters rows with the exact same partition key onto the same executor nodes.
   4. **Physical Directory Creation**: The worker node communicates with Azure Storage (ADLS Gen2) via the Hadoop FileSystem API. If the specific directory path doesn't exist, it creates a physical directory:
   abfss://container@storage.dfs.core.windows.net/user_events/event_date=2026-10-03/
   5. **Isolated Writing**: The worker node streams only the matching rows into Parquet data files inside that specific subdirectory.

### Step 2: Registering with the Delta Log
Once the files are physically written to their respective folders, the driver node commits this layout to the Delta Lake Transaction Log (_delta_log/00000X.json).

Instead of treating the partition columns as part of the internal Parquet data schema, Delta Lake explicitly separates them into partitionValues metadata mappings:
```json
{
  "add": {
    "path": "event_date=2026-10-03/part-00000-xyz.parquet",
    "partitionValues": {
      "event_date": "2026-10-03"
    },
    "size": 1245320,
    "modificationTime": 1791050220000
  }
}
```
**Note:** Because the partition values are explicitly written into the directory names and transaction log metadata, Databricks actually drops the partition column from the internal physical schema of the Parquet files themselves to save disk space.

### Step 3: The Read Phase – Compile-Time Partition Pruning
The real programmatic magic of PARTITION BY happens during query planning via a optimization technique called Static or _Dynamic Partition Pruning (DPP)_.

When a user submits a query:
```sql
SELECT * FROM main.prod.user_events WHERE event_date = '2026-10-03';
```
#### The Algorithmic Evaluation:

   1. **Catalyst Optimizer Interception**: Spark's Catalyst Optimizer parses the abstract syntax tree (AST) of the SQL statement and isolates the WHERE clause expressions.
   2. **Metadata Filter Execution**: Before allocating any compute tasks or reading a single byte from storage, Spark reads the ultra-lightweight Delta Lake JSON transaction log.
   3. **Set Intersection**: Spark performs a quick relational filter directly on the JSON log strings:
   `{Match} = {Paths where partitionValues.event_date == '2026-10-03' }`
   4. **Folder Isolation (Pruning)**: If your table contains 5 years of data (1,825 daily partition folders), Spark instantly drops 1,824 directory pointers out of memory.

### Why it Fails on High-Cardinality Data (The O(N) Metadata Problem)
- Programmatically, if you make the mistake of running PARTITION BY (user_id) where there are 10 million users, the storage engine is forced to track 10 million distinct directory strings.
- When a query runs, the cluster's driver node must load an incredibly massive list of file paths into its JVM heap memory just to perform basic string filtering. This results in Driver Out Of Memory (OOM) errors and extreme metadata bottlenecks, which is exactly why ZORDER BY is used for high-cardinality values instead.

---


