# Apache Spark & Hadoop — A Deep, Beginner-Friendly Guide (Using This Repository)

> **Who is this for?**
> Someone who has never seriously used Spark or Hadoop, but knows a bit of Java.
> By the end you should understand **what** Spark and Hadoop are, **why** they exist,
> **how** they work internally, and be able to point at **real code in this repository**
> and say "ah, *that* is a partition, *that* is a shuffle, *that* is the driver, *that* is an executor."
>
> Every abstract concept below is followed by a **"In this repo"** box that links the idea to a real file.

---

## Table of Contents

1. [The Big Picture: Why Do Spark and Hadoop Exist?](#1-the-big-picture)
2. [Hadoop Explained](#2-hadoop-explained)
   - 2.1 HDFS (the storage layer)
   - 2.2 MapReduce (the original compute layer)
   - 2.3 YARN (the resource manager)
   - 2.4 The Hadoop *ecosystem* vs Hadoop *core*
   - 2.5 The Hadoop `FileSystem` abstraction (and why S3 counts as "Hadoop")
3. [Apache Spark Explained](#3-apache-spark-explained)
   - 3.1 What problem Spark solves over MapReduce
   - 3.2 The cluster anatomy: Driver, Executors, Cluster Manager
   - 3.3 RDD → DataFrame → Dataset
   - 3.4 Transformations vs Actions (lazy evaluation)
   - 3.5 Partitions — the unit of parallelism
   - 3.6 The Shuffle — the most important performance concept
   - 3.7 Jobs, Stages, Tasks
   - 3.8 `persist()` / caching
   - 3.9 Broadcast variables
   - 3.10 Serialization (Kryo)
   - 3.11 Adaptive Query Execution (AQE)
4. [Spark Structured Streaming](#4-spark-structured-streaming)
5. [Apache Hudi — The Missing Piece](#5-apache-hudi)
6. [End-to-End Walkthrough of THIS Pipeline](#6-end-to-end-walkthrough)
7. [How to Read a Spark Job Like a Pro](#7-how-to-read-a-spark-job)
8. [The Dependency Stack (pom.xml decoded)](#8-the-dependency-stack)
9. [Glossary](#9-glossary)
10. [Mental Model Cheat-Sheet](#10-cheat-sheet)

---

## 1. The Big Picture

### The problem
Imagine you have **500 million bill records** sitting in cloud storage, and you need to:
read them, clean them, group them per customer, and send notifications.

One laptop cannot do this:
- The data doesn't fit in one machine's RAM.
- Even if it did, one CPU processing 500M records serially would take hours or days.

The solution is **distributed computing**: split the data across many machines, process the
pieces **in parallel**, then combine the results. But distributed computing is *hard*:
machines crash, networks are slow, and coordinating hundreds of workers is error-prone.

**Hadoop** and **Spark** are frameworks that hide this difficulty. You write code that *looks*
like it processes one big dataset; the framework secretly splits it, ships it to many machines,
handles failures, and reassembles the answer.

### The one-sentence definitions
- **Hadoop** = a system for **storing** huge files across many machines (HDFS) and **processing**
  them across many machines (MapReduce/YARN). It was the first widely-used "big data" platform.
- **Spark** = a much **faster, more flexible compute engine** that replaced Hadoop's MapReduce.
  It still uses Hadoop's *storage ideas* and *file formats*, but does the computation in memory
  and with a far nicer programming model.

> **Analogy:** Hadoop is the *warehouse + forklift fleet* (store lots of boxes, move them around).
> Spark is a *much smarter, faster logistics brain* that plans the moves optimally and keeps
> hot items on a fast conveyor belt (RAM) instead of walking to the shelf every time.

> **In this repo:** This project is a **Spark application** that reads/writes data using **Hadoop's
> filesystem layer** (talking to Amazon S3), stores data in **Apache Hudi** table format, and
> streams from **Kafka**. You will see all of these below. The main entry points are:
> - `src/main/java/com/notification/KafkaStreamingHudiWriter.java` — streaming ingest (Kafka → S3/Hudi)
> - `src/main/java/com/notification/BillDataIngestionFromCSV.java` — batch ingest (CSV → S3/Hudi)
> - `src/main/java/com/notification/SmsAggregatorJob.java` — batch aggregate (S3/Hudi → Kafka + Cassandra)

---

## 2. Hadoop Explained

Hadoop core has **three** pieces. Modern Spark jobs (like this repo) usually use only piece #1
(the storage/filesystem abstraction) and skip #2 and #3 — but you must understand all three to
know *where Spark came from* and *why the code looks the way it does*.

```
        ┌─────────────────────────────────────────────┐
        │                  HADOOP                      │
        ├───────────────┬───────────────┬──────────────┤
        │     HDFS      │   MapReduce   │     YARN     │
        │ (storage)     │   (compute)   │ (scheduling) │
        └───────────────┴───────────────┴──────────────┘
```

### 2.1 HDFS — Hadoop Distributed File System (the storage layer)

**Problem it solves:** store a file that is bigger than any single disk, and survive disk failures.

**How it works:**
- A big file (say 1 TB) is chopped into **blocks** (default 128 MB each).
- Each block is copied (**replicated**, usually 3×) onto different machines (**DataNodes**).
- A master server (**NameNode**) remembers *which blocks live on which machines* (the metadata/map).

```
  file "bills.parquet" (1 GB)
        │  split into blocks
        ▼
  [blk1][blk2][blk3][blk4] ... [blk8]
        │  each block replicated 3× across DataNodes
        ▼
  DataNode A: blk1, blk4, blk7
  DataNode B: blk1, blk2, blk5
  DataNode C: blk2, blk3, blk6 ...
        ▲
  NameNode knows the full map (which block → which nodes)
```

**Why replication?** If DataNode A dies, blk1 still exists on B. No data lost. This is
**fault tolerance** — a core theme of all big-data systems.

**Data locality:** Instead of moving 128 MB of data to where the code runs, Hadoop tries to
**move the code to the machine that already has the data**. Moving code (a few KB) is far
cheaper than moving data (many MB). This principle drives how Spark schedules work too.

> **In this repo:** There is explicit HDFS configuration in
> `src/main/java/com/notification/config/HdfsConfig.java`. Look at lines 20–27:
> ```java
> builder.config("spark.hadoop.fs.defaultFS", hdfsUri)
>        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
>        .config("spark.hadoop.dfs.client.failover.proxy.provider",
>               "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
>        .config("spark.hadoop.dfs.ha.automatic-failover.enabled", "true")
> ```
> - `fs.defaultFS` = the URI of the NameNode (the "address" of the whole filesystem).
> - `ConfiguredFailoverProxyProvider` + `automatic-failover` = **HA (High Availability)**: there are
>   *two* NameNodes (active + standby); if the active one dies, clients auto-switch to the standby.
> - This is classic Hadoop HDFS setup. In production this project mostly uses **S3 instead of HDFS**
>   (see 2.5), but the code keeps HDFS support available.

### 2.2 MapReduce — the original compute layer

Before Spark, you processed HDFS data by writing **MapReduce** programs. The model:

1. **Map**: run a function on each record independently, emitting key→value pairs.
2. **Shuffle**: group all values with the same key together (moving data across the network).
3. **Reduce**: run a function on each key's grouped values to produce the final result.

Classic example — **word count**:
- Map: for each word in a line, emit `(word, 1)`.
- Shuffle: bring all `(the, 1)` pairs to the same reducer.
- Reduce: sum them → `(the, 4823)`.

**Why MapReduce fell out of favor:** every step writes its intermediate results **to disk (HDFS)**
before the next step reads them back. For multi-step jobs this is painfully slow. Also the API is
low-level and verbose.

> **In this repo:** There is **no** MapReduce code — this is a Spark project. But notice the shape
> of `SmsAggregatorJob.aggregateDataByCustomerAndService()`
> (`src/main/java/com/notification/SmsAggregatorJob.java:307`):
> ```java
> data.groupBy("customerId", "service")
>     .agg(functions.collect_list(functions.struct(...)).as("combined"))
> ```
> This is conceptually a **map** (build the struct per row) + **shuffle** (`groupBy` moves rows so
> the same customerId lands together) + **reduce** (`collect_list` gathers them). Spark expresses
> the *same idea* as MapReduce but far more concisely and runs it in memory. You are looking at
> "MapReduce, evolved."

### 2.3 YARN — Yet Another Resource Negotiator (the scheduler)

**Problem it solves:** a cluster has finite CPU and RAM; many jobs want to run. Someone must
decide "job A gets 10 containers of 4 GB each on these nodes." That's YARN.

- **ResourceManager** — the cluster-wide boss that hands out resources.
- **NodeManager** — an agent on each machine that launches and monitors **containers** (a slice
  of CPU+RAM).
- **ApplicationMaster** — a per-job coordinator that asks the ResourceManager for containers.

Spark can run on YARN, but also on **Kubernetes**, **Mesos**, **Spark Standalone**, or **local mode**
(one JVM, for development). YARN is just one option.

> **In this repo:** The run script `scripts/run-bill-ingestion.sh` sets:
> ```bash
> SPARK_MASTER="local[*]"
> ```
> `local[*]` means "run Spark inside a single JVM, using **all** CPU cores as threads" — no cluster,
> no YARN. This is how you develop and test on a laptop. In production this same JAR would be
> submitted with a real master (`yarn`, `k8s://…`, or on AWS EMR/EMR-Serverless). The code doesn't
> change — only the *master URL* does. That portability is a key Spark selling point.

### 2.4 Hadoop *core* vs the Hadoop *ecosystem*

People say "Hadoop" loosely. Two meanings:
- **Hadoop core** = HDFS + MapReduce + YARN (the three above).
- **Hadoop ecosystem** = the huge family of tools built *around* it: Hive (SQL over big data),
  HBase (NoSQL DB), **Spark**, Kafka, Sqoop, Oozie, **Hudi**, Iceberg, etc.

This repo uses **ecosystem** tools (Spark, Hudi, Kafka) that *depend on Hadoop's libraries* for
filesystem access and file formats, without necessarily using HDFS or MapReduce or YARN.

### 2.5 The Hadoop `FileSystem` abstraction (why S3 is treated as "Hadoop")

This is the single most important Hadoop concept for *this* codebase.

Hadoop defines a Java interface `org.apache.hadoop.fs.FileSystem`. Anything that implements it can
be read/written by Hadoop and Spark using the *same code*. Implementations include:
- `hdfs://…`  → real HDFS
- `file://…`  → your local disk
- `s3a://…`   → Amazon S3 (implemented by the `hadoop-aws` library, class `S3AFileSystem`)

So when this project writes to `s3a://bucket/path`, Spark thinks it's just "a Hadoop filesystem."
That's why an S3-based project still pulls in **Hadoop** dependencies.

> **In this repo:**
> - `pom.xml` declares `hadoop-aws`, `hadoop-common`, `hadoop-client` — these provide the
>   `FileSystem` interface and the S3 implementation.
> - `src/main/java/com/notification/config/S3Config.java:25` wires S3 into Spark as a Hadoop FS:
>   ```java
>   builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
>          .config("spark.hadoop.fs.s3a.endpoint", "s3." + region + ".amazonaws.com")
>          .config("spark.hadoop.fs.s3a.fast.upload", "true")
>   ```
>   Every `spark.hadoop.*` key is passed straight through to Hadoop's configuration. `fs.s3a.impl`
>   literally says "when you see an `s3a://` path, use the `S3AFileSystem` class."
> - `src/main/java/com/notification/util/HudiUtils.java:44` uses the raw Hadoop API directly to
>   check whether an S3 partition folder exists:
>   ```java
>   FileSystem fs = FileSystem.get(new URI(basePath), spark.sparkContext().hadoopConfiguration());
>   ...
>   if (fs.exists(s3Path)) { partitionPaths.add(partitionPath); }
>   ```
>   `FileSystem`, `Path`, `spark.sparkContext().hadoopConfiguration()` are **all pure Hadoop APIs**,
>   used here to talk to **S3**. This is the clearest example in the repo that "Hadoop the library"
>   ≠ "HDFS the storage." You are using Hadoop code to touch Amazon S3.

---

## 3. Apache Spark Explained

### 3.1 What Spark adds over MapReduce

| Concern | MapReduce | Spark |
|---|---|---|
| Intermediate data | Written to disk between steps | Kept in **RAM** (memory) when possible |
| Programming model | Verbose Map/Reduce classes | High-level `DataFrame`/SQL/functional API |
| Multi-step jobs | Slow (disk between every step) | Fast (chained in memory) |
| Use cases | Batch only | Batch **+ streaming + ML + SQL + graph** |
| Fault tolerance | Re-run failed tasks | **Lineage** — recompute only lost partitions |

The headline: **Spark can be 10–100× faster** for iterative/multi-step workloads because it avoids
round-tripping to disk, and it's far easier to program.

### 3.2 Cluster anatomy — Driver, Executors, Cluster Manager

This is *the* mental model you must internalize. A Spark application has:

```
                         ┌────────────────────────────────┐
                         │            DRIVER              │
                         │  (runs your main() method)     │
                         │  - builds the plan             │
                         │  - holds SparkSession          │
                         │  - schedules tasks             │
                         │  - collects final results      │
                         └───────────────┬────────────────┘
                                         │ sends tasks
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼
      ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
      │  EXECUTOR 1   │          │  EXECUTOR 2   │          │  EXECUTOR N   │
      │  (a JVM)      │          │  (a JVM)      │          │  (a JVM)      │
      │  runs tasks   │          │  runs tasks   │          │  runs tasks   │
      │  on partitions│          │  on partitions│          │  on partitions│
      │  caches data  │          │  caches data  │          │  caches data  │
      └───────────────┘          └───────────────┘          └───────────────┘
```

- **Driver**: the "brain." It runs your `main()`, builds the execution plan, breaks work into
  tasks, and sends them to executors. There is **exactly one** driver per application.
- **Executor**: a "worker" JVM running on a cluster machine. It executes the tasks the driver sends,
  processing one **partition** of data at a time, and can cache data in its memory. There are
  **many** executors.
- **Cluster Manager** (YARN/K8s/Standalone/local): hands executors to the driver at startup.

**Critical rule:** Code inside operations like `mapPartitions`, `foreachPartition`, `map`, filters,
etc. runs **on the executors**, not the driver. Code in your `main()` (outside those operations)
runs **on the driver**. Mixing this up is the #1 source of confusion for beginners.

> **In this repo — driver code vs executor code, side by side:**
> In `SmsAggregatorJob.java`:
> - **Driver code** (runs once, in `main`): creating the session, loading config, calling
>   `.count()`, `.show()`. E.g. line 139 `SparkUtils.createSparkSession(...)`, line 163
>   `long count = aggregatedData.count();`.
> - **Executor code** (runs in parallel, once per partition): the body passed to `mapPartitions`
>   at line 176:
>   ```java
>   Dataset<Row> processedNotifications = aggregatedData.mapPartitions(
>       (MapPartitionsFunction<Row, Row>) partition -> {
>           ...
>           return processPartitionWithReturn(scalaIterator, configBroadcast);
>       }, getProcessedNotificationEncoder());
>   ```
>   Everything inside `processPartitionWithReturn` (opening Kafka producers, Cassandra writers,
>   validating records) executes **on the executors**, in parallel, once per partition. That's why
>   it carefully creates *per-executor* connections via `ConnectionManager` — because that code is
>   literally running on many different machines at once.

### 3.3 RDD → DataFrame → Dataset (the data abstractions)

Spark has three layers of data abstraction, from oldest/lowest to newest/highest:

1. **RDD (Resilient Distributed Dataset)** — the original. A distributed collection of Java objects,
   split into partitions. Low-level, no built-in schema, no automatic optimization. "Resilient"
   because it can rebuild lost partitions from its **lineage** (the recipe of operations that
   produced it).

2. **DataFrame** = `Dataset<Row>` — a distributed table with **named, typed columns** (a schema),
   like a spreadsheet or SQL table. This is what most modern Spark code uses. Spark's **Catalyst
   optimizer** can rewrite your DataFrame operations into a faster plan because it understands the
   schema.

3. **Dataset<T>** — like a DataFrame but strongly typed to a Java/Scala class `T` (e.g.
   `Dataset<BillDataPayload>`), giving compile-time type safety.

> **In this repo — all three appear:**
> - **DataFrame** (`Dataset<Row>`) everywhere, e.g. `SparkUtils.readCsvData` returns `Dataset<Row>`
>   (`src/main/java/com/notification/util/SparkUtils.java:109`).
> - **Typed Dataset**: `BillDataIngestionFromCSV.java:75`:
>   ```java
>   Dataset<BillDataPayload> enrichedData = csvData.mapPartitions(..., Encoders.bean(BillDataPayload.class));
>   ```
>   Here the DataFrame of raw rows is turned into a strongly-typed `Dataset<BillDataPayload>`.
> - **RDD** peeking through: whenever you see `data.rdd().getNumPartitions()` (e.g.
>   `SparkUtils.java:128`), that's dropping down to the underlying RDD to inspect partitions. A
>   DataFrame is backed by an RDD.

**Encoders:** To move data between the JVM object world and Spark's internal binary format, Spark
needs an **Encoder**. `Encoders.bean(BillDataPayload.class)` and `Encoders.kryo(Row.class)`
(`SmsAggregatorJob.java:683`) are examples — they tell Spark *how to serialize* your objects.

### 3.4 Transformations vs Actions (Lazy Evaluation)

Spark operations come in two flavors:

- **Transformations** — *describe* a new dataset from an existing one, but **do nothing yet**.
  They are **lazy**. Examples: `filter`, `select`, `groupBy`, `map`, `withColumn`, `repartition`,
  `mapPartitions`, `union`, `agg`.
- **Actions** — *trigger actual computation* and return a value to the driver or write output.
  Examples: `count`, `collect`, `show`, `save`/`write`, `foreachPartition`, `first`, `take`.

**Lazy evaluation** means: when you chain `.filter().select().groupBy()`, Spark just builds a
**plan** (a recipe). Nothing runs until you call an **action**. Then Spark looks at the whole recipe,
**optimizes** it, and executes it efficiently.

> **Why this matters:** it lets Spark optimize across your whole pipeline (e.g. push a filter down
> to read less data). But it also has a **gotcha**: each action **re-runs** the whole chain from
> scratch unless you cache. See 3.8.

> **In this repo — spotting the boundary:**
> In `SmsAggregatorJob.loadAndAggregateData` (`SmsAggregatorJob.java:285`):
> ```java
> Dataset<Row> data = HudiUtils.loadHudiPartitions(spark, getHudiS3Path(), dueDates); // transformation (lazy)
> logger.info("Data schema: {}", data.schema().prettyJson());   // schema is known without running
> logger.info("total count in loadAndAggregateData is " + data.count());  // ACTION → triggers a full read
> data.show(false);   // ACTION → triggers computation again
> return aggregateDataByCustomerAndService(data);  // more transformations (lazy)
> ```
> The `.count()` and `.show()` are **actions**; each one forces Spark to actually read from S3.
> `groupBy(...).agg(...)` inside `aggregateDataByCustomerAndService` is a **transformation** — it
> won't run until a later action (like the `.count()` at line 163 in `main`).

### 3.5 Partitions — the unit of parallelism

A **partition** is a chunk of the dataset that a **single task** processes on a **single core**.
If your DataFrame has 240 partitions, Spark can process up to 240 chunks in parallel (limited by
how many cores your executors actually have).

- **Too few partitions** → underuse the cluster (idle cores), and each task handles too much data
  (risk of running out of memory).
- **Too many partitions** → overhead of scheduling tiny tasks dominates.
- Rule of thumb: aim for partitions of ~100–200 MB, and roughly 2–4× the total core count.

You control partition count with `repartition(n)` (full shuffle, exact count) or `coalesce(n)`
(cheaper, only reduces count, avoids full shuffle).

> **In this repo — deliberate partition tuning:**
> - `BillDataIngestionFromCSV.java:53`:
>   ```java
>   Dataset<Row> csvData = SparkUtils.readCsvData(spark, csvS3Path).repartition(240);
>   ```
>   The CSV is forced into **240 partitions** so 240 tasks can enrich rows in parallel (each row
>   does MySQL/Cassandra lookups, so parallelism matters a lot here).
> - `SmsAggregatorJob.java:326`: `...as("combined")).repartition(240);` — after the `groupBy`, the
>   aggregated data is re-partitioned to 240 so downstream `mapPartitions` work is spread evenly.
> - Logging the count everywhere: `data.rdd().getNumPartitions()` appears repeatedly (e.g.
>   `SmsAggregatorJob.java:169`, `292`, `328`) so operators can *see* the parallelism in the logs.

### 3.6 The Shuffle — the most important performance concept

A **shuffle** happens when Spark must **move data across the network** so that related records end
up in the same partition. This occurs for operations like `groupBy`, `join`, `repartition`,
`distinct`, and aggregations.

Why it's expensive: it writes intermediate data to disk and sends it over the network between
executors — orders of magnitude slower than in-memory work. **Minimizing shuffles is the heart of
Spark performance tuning.**

```
   Before groupBy (data scattered by arrival):
   Partition A: cust1, cust2, cust1
   Partition B: cust2, cust3, cust1

            │  groupBy("customerId")  ── SHUFFLE (network move) ──▶
            ▼
   After: all rows for a customer in one partition
   Partition X: cust1, cust1, cust1
   Partition Y: cust2, cust2
   Partition Z: cust3
```

> **In this repo — where the shuffles are:**
> - `SmsAggregatorJob.java:309` `data.groupBy("customerId", "service").agg(collect_list(...))` — this
>   is a **shuffle**: every row for the same `(customerId, service)` must be gathered onto one
>   partition so `collect_list` can build the combined array. This is the single biggest network
>   operation in the aggregation job.
> - `.repartition(240)` (two places) is an **explicit shuffle** — you're asking Spark to reshuffle
>   into exactly 240 partitions.
> - Hudi's write options like `"hoodie.upsert.shuffle.parallelism", "400"`
>   (`KafkaStreamingHudiWriter.java:833`) tune how many partitions Hudi uses *during its own internal
>   shuffle* when indexing/upserting records.

### 3.7 Jobs, Stages, Tasks (how an action becomes work)

When you call an **action**, Spark:
1. Creates a **Job** for that action.
2. Splits the job into **Stages**, cut at each **shuffle boundary** (a stage is a run of
   transformations with no shuffle in the middle).
3. Splits each stage into **Tasks** — **one task per partition**. Tasks are the actual units sent
   to executors.

```
   ACTION (e.g. count())
     └── JOB
          ├── STAGE 1 (read + map)      → 240 tasks (one per partition)
          │      ⇣ shuffle (groupBy)
          └── STAGE 2 (aggregate)       → 240 tasks
```

So "240 partitions" means "240 tasks per stage," which is why partition count directly controls
parallelism. You watch all of this in the **Spark UI** (usually at `http://<driver>:4040`).

> **In this repo:** The `spark-events/` directory contains **Spark event logs** — the recorded
> history of jobs/stages/tasks from past runs. (Event logging is toggled by
> `spark.eventLog.enabled`, defined as a constant in `SparkConstants.java:50`; in `SparkUtils` it's
> currently set to `false`, line 47, and re-enabled elsewhere/on the cluster.) Those files are what
> the **Spark History Server** reads to let you replay a finished job's stages and tasks.

### 3.8 `persist()` / caching — avoid recomputing

Because of lazy evaluation, **every action re-executes the full lineage**. If you call `.count()`
then `.show()` then write — each one re-reads S3 and re-aggregates. To avoid that, **cache** the
result in executor memory with `.persist()` (or `.cache()`), so subsequent actions reuse it.

> **In this repo — a textbook example:**
> `SmsAggregatorJob.java:162`:
> ```java
> aggregatedData.persist();  // Spark will cache result in executor memory
> long count = aggregatedData.count();  // Executes aggregation only once
> aggregatedData.show(false);           // Reuses the cached result — no recompute
> ```
> The comment literally says "Executes aggregation only once." Without `persist()`, the `count()`
> and `show()` (and the later `mapPartitions`) would each re-run the expensive S3 read + groupBy.
> The same pattern repeats at line 185 for `processedNotifications`.

### 3.9 Broadcast variables — sharing read-only data efficiently

Sometimes every task needs the *same* piece of read-only data (a config, a lookup map). If you just
reference a variable, Spark ships a copy **with every task** (wasteful). A **broadcast variable**
ships it **once per executor** and all tasks on that executor share it.

> **In this repo — clear and central:**
> `SmsAggregatorJob.java:173`:
> ```java
> final Broadcast<ExecutorConfig> configBroadcast =
>     spark.sparkContext().broadcast(executorConfig, ...ClassTag...apply(ExecutorConfig.class));
> ```
> The `ExecutorConfig` (DB credentials, Kafka/Cassandra settings, feature flags) is **broadcast once
> to every executor**. Then inside the per-partition code it's read back:
> `configBroadcast.value()` (`SmsAggregatorJob.java:434`). At the end it's cleaned up:
> `configBroadcast.destroy();` (line 203). Same pattern in `BillDataIngestionFromCSV.java:70`.
> This is the idiomatic way to push configuration to distributed workers.

### 3.10 Serialization (Kryo)

To send tasks and data between the driver and executors (across the network), objects must be
**serialized** (turned into bytes). Java's built-in serialization is slow and bulky. Spark supports
**Kryo**, a much faster/smaller serializer.

> **In this repo:**
> `SparkUtils.java:30` (via constants): `.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")`.
> The literal strings are in `SparkConstants.java:9-10`. Also `Encoders.kryo(Row.class)` at
> `SmsAggregatorJob.java:683` uses Kryo to encode arbitrary `Row` objects for the processed-
> notifications dataset. Choosing Kryo is a standard performance best-practice.

### 3.11 Adaptive Query Execution (AQE)

AQE lets Spark **re-optimize the plan at runtime** using actual data statistics (not just estimates).
It can, mid-job: coalesce too-many small shuffle partitions into fewer, switch join strategies, and
split skewed partitions (where one key has way more data than others).

> **In this repo — AQE fully enabled:**
> `SparkUtils.java:35-39`:
> ```java
> .config("spark.sql.adaptive.enabled", "true")
> .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
> .config("spark.sql.adaptive.skewJoin.enabled", "true")
> .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
> .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")  // target ~128MB per partition
> ```
> The advisory size of **128 MB** matches the classic HDFS block size — a sensible target chunk. The
> literal keys are in `SparkConstants.java:22-27`.

---

## 4. Spark Structured Streaming

Everything above is **batch** (process a fixed dataset, finish, exit). Spark also does **streaming**:
process data **continuously** as it arrives.

**Structured Streaming** model: treat an unbounded stream as an **infinite table** that keeps
gaining rows. You write almost the *same* DataFrame code as batch; Spark runs it repeatedly in
tiny **micro-batches** (e.g. every 30 seconds), each processing the newly-arrived data.

Key streaming concepts:
- **Source** — where data comes from (here: **Kafka**).
- **Trigger** — how often to run a micro-batch (`ProcessingTime(30s)`).
- **Sink** — where output goes (here: **Hudi on S3**).
- **Checkpoint** — a folder where Spark saves its progress (which Kafka offsets it has consumed) so
  that after a crash/restart it resumes **exactly where it left off** (fault tolerance).
- **Output mode** — `append` / `update` / `complete`.

> **In this repo — the whole streaming job:** `KafkaStreamingHudiWriter.java`.
> - **Read from Kafka** (`readStream`, line 197):
>   ```java
>   spark.readStream().format("kafka")
>        .option("kafka.bootstrap.servers", bootstrapServers)
>        .option("subscribe", topic)
>        .option("startingOffsets", ...)
>        .option("maxOffsetsPerTrigger", "500000")   // cap 500K records/micro-batch → avoid OOM
>        .option("minPartitions", "50")               // force parallelism
>        .load();
>   ```
> - **Transform** the raw Kafka JSON into clean columns with pure DataFrame operations:
>   `transformMaxwellData` (line 247) and `transformCdcRecoveryData` (line 397). Note the use of
>   `from_json` (parse JSON string → struct using a declared schema, line 265), `withColumn`,
>   `filter`, `select`, `explode` (line 442, to turn one partition-change event into *two* rows:
>   a delete on the old partition + an insert on the new one), and `unionByName` (line 96, to merge
>   two streams).
> - **Write to Hudi** (`writeStream`, line 783) with a **checkpoint** (line 851
>   `.option("checkpointLocation", checkpointPath)`) and a **trigger**
>   (line 850 `.trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))`).
> - **Two modes** (line 127+): *batch mode* (`awaitTermination` after draining), *time-limited mode*
>   (`awaitAnyTermination(minutes)`), or *continuous mode* (run forever). One code path, three
>   operational behaviors.
>
> The big comment block at lines 88–95 is worth reading: it explains a real production decision —
> they deliberately **avoid stateful stream deduplication** (which stores state in the checkpoint on
> S3 and can corrupt) and instead let **Hudi's upsert with a precombine key** decide the final record.
> That's a great example of design trade-offs in real streaming systems.

---

## 5. Apache Hudi — The Missing Piece

Plain files on S3/HDFS have a big limitation: you can't easily **update** or **delete** a single
record, and you get no **transactions**. If two jobs write at once, or a job crashes mid-write, you
can end up with half-written, corrupt data. Also, re-reading a whole table to change one row is
wasteful.

**Apache Hudi** (Hadoop Upserts, Deletes, and Incrementals) is a **table format** layered on top of
files in S3/HDFS. It adds:
- **Upserts** (insert-or-update by a **record key**) and **deletes**.
- **ACID transactions** via a **commit timeline** (an ordered log of atomic commits).
- **Deduplication** via a **precombine key** (when two records share a key, the one with the larger
  precombine value wins).
- **Partitioning** (data laid out into folders by a partition column, so queries skip irrelevant data).
- **Indexes** (e.g. **BLOOM**) to quickly find which file contains a record key during upsert.
- Two table types: **Copy-on-Write (COW)** — rewrites files on update, fast reads; and
  **Merge-on-Read (MOR)** — appends delta logs, fast writes.

> **In this repo — Hudi is central. Key concepts mapped to code:**
>
> **(a) Writing a Hudi table** — `HudiUtils.writeHudiData` (`HudiUtils.java:87`):
> ```java
> data.write().format("org.apache.hudi")
>     .option("hoodie.table.name", tableName)
>     .option("hoodie.datasource.write.recordkey.field", ...)      // the unique key
>     .option("hoodie.datasource.write.partitionpath.field", "dueDatePartition")  // folder layout
>     .option("hoodie.datasource.write.precombine.field", ...)     // conflict winner
>     .option("hoodie.datasource.write.operation", ...)            // upsert / insert / bulk_insert
>     .mode("overwrite").save(destinationPath);
> ```
>
> **(b) Streaming upsert with BLOOM index** — `KafkaStreamingHudiWriter.writeToHudiWithBloomIndex`
> (`KafkaStreamingHudiWriter.java:783`):
> ```java
> .option(TABLE_TYPE_OPT_KEY(), COW_TABLE_TYPE_OPT_VAL())               // Copy-on-Write
> .option(RECORDKEY_FIELD_OPT_KEY(), "customerId,service,rechargeNumber,operator")  // composite key
> .option(PARTITIONPATH_FIELD_OPT_KEY(), "dueDatePartition")
> .option(PRECOMBINE_FIELD_OPT_KEY(), "precombineKey")
> .option("hoodie.index.type", "BLOOM")
> .option(KEYGENERATOR_CLASS_OPT_KEY(), "org.apache.hudi.keygen.ComplexKeyGenerator")  // multi-field key
> ```
> - The **record key** is a *composite* of four fields — that's the unique identity of a bill.
> - The **partition path** is `dueDatePartition` — so on S3 you get folders like
>   `dueDatePartition=2025-03-01/` (Hive-style partitioning, enabled by `HIVE_STYLE_PARTITIONING`).
> - The **precombine key** (`precombineKey`) is a cleverly-built number:
>   `(sourcePriority * 10^15) + updated_at_millis` (built at `KafkaStreamingHudiWriter.java:343`).
>   This guarantees: records from the higher-priority source (`REMINDER_MAXWELL`, priority 2) always
>   beat the recovery source (priority 1); and within the same source, the newer `updated_at` wins.
>   **This is exactly how Hudi resolves conflicts** — and this repo encodes real business rules into
>   that single number.
> - `_hoodie_is_deleted` (line 352) is Hudi's magic column: when `true`, Hudi **deletes** that record
>   on upsert. That's how CDC delete events remove bills.
>
> **(c) Reading specific Hudi partitions** — `HudiUtils.loadHudiPartitions` (`HudiUtils.java:34`):
> ```java
> spark.read().format("org.apache.hudi")
>      .option("hoodie.datasource.read.paths", String.join(",", partitionPaths))
>      .load();
> ```
> It only reads the partition folders for the due-dates it cares about (checking each exists first
> with the Hadoop `FileSystem` API) — this is **partition pruning**, avoiding a full-table scan.
>
> **(d) COW vs the config** — `S3StorageWriter.java:60` also uses
> `COW_TABLE_TYPE_OPT_VAL()` (Copy-on-Write). The README's "Hudi Configuration" section documents
> the same choices in plain English.

---

## 6. End-to-End Walkthrough of THIS Pipeline

Now let's connect everything into the actual data flow of this project. There are two ingestion
paths that both land in **the same Hudi table on S3**, and one aggregation job that consumes it.

```
   ┌──────────────────────────────────────────────────────────────────────────┐
   │  PATH A — STREAMING INGEST  (KafkaStreamingHudiWriter.java)               │
   │                                                                          │
   │  Kafka: REMINDER_MAXWELL ──┐                                             │
   │                            ├─▶ Spark Structured Streaming               │
   │  Kafka: CDC_RECOVERY ──────┘   (parse JSON, clean, dedup-by-precombine) │
   │                                        │                                │
   │                                        ▼                                │
   │                      Hudi UPSERT (COW, BLOOM index)                     │
   │                                        │                                │
   └────────────────────────────────────────┼───────────────────────────────┘
                                             ▼
                    ┌───────────────────────────────────────────┐
                    │   S3  Hudi table: combined_notification_raw │
                    │   partitioned by  dueDatePartition=yyyy-MM-dd│
                    └───────────────────────────────────────────┘
                                             ▲
   ┌─────────────────────────────────────────┼───────────────────────────────┐
   │  PATH B — BATCH INGEST  (BillDataIngestionFromCSV.java)                  │
   │  CSV on S3 ─▶ repartition(240) ─▶ mapPartitions (enrich from MySQL /     │
   │              Cassandra) ─▶ Hudi write                                    │
   └──────────────────────────────────────────────────────────────────────────┘

                                             │  (later, as a batch job)
                                             ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │  AGGREGATION  (SmsAggregatorJob.java)                                    │
   │  read chosen dueDate partitions from Hudi                                │
   │     │                                                                    │
   │     ▼  groupBy(customerId, service) + collect_list   ── SHUFFLE ──       │
   │  aggregated rows (one per customer+service, with a "combined" array)     │
   │     │  repartition(240) → mapPartitions (runs on executors)             │
   │     ▼  validate, filter (overdue, size, active/CVR), batch of 100        │
   │  ┌───────────────┬───────────────────┬────────────────────────────┐     │
   │  ▼               ▼                   ▼                            ▼      │
   │ Kafka          Cassandra        S3 (processed-                          │
   │ (notifications) (combine_        notification, for validation)          │
   │                  notification)                                          │
   └──────────────────────────────────────────────────────────────────────────┘
```

### Step-by-step, with the code

**PATH A — Streaming (`KafkaStreamingHudiWriter.java`)**
1. `createStreamingSparkSession()` (line 158) builds a `SparkSession` with Kafka + Hudi + S3 support.
2. `readFromKafkaTopic()` (line 191) opens a streaming DataFrame on each Kafka topic.
3. `transformMaxwellData()` / `transformCdcRecoveryData()` (lines 247, 397) parse the JSON and
   normalize both feeds into one **canonical schema** (`toCanonicalStreamSchema`, line 617).
4. `unionByName` (line 96) merges them; `writeToHudiWithBloomIndex` (line 783) upserts into the
   Hudi table every 30 seconds, checkpointing progress to S3.

**PATH B — Batch CSV ingest (`BillDataIngestionFromCSV.java`)**
1. `SparkUtils.readCsvData(...)` (returns a `Dataset<Row>` with an explicit schema, no inference).
2. `.repartition(240)` for parallelism.
3. `mapPartitions(...)` (line 75): on each executor, for each row, **enrich** it by looking up extra
   bill data in **MySQL first, then Cassandra** (`enrichRowData`, line 208). Note the per-executor
   singleton pattern (line 128) so DB connections are created **once per executor**, not per row.
4. `HudiUtils.writeHudiData(...)` writes the enriched data to the same Hudi table.

**AGGREGATION (`SmsAggregatorJob.java`)**
1. `generateDueDatesFromOperators(...)` (line 249) computes which due-date partitions to process.
2. `loadAndAggregateData(...)` (line 285) reads only those Hudi partitions, then
   `aggregateDataByCustomerAndService` (line 307) does the big `groupBy(...).collect_list(...)`.
3. `persist()` + `count()` + `show()` (lines 162–164) materialize once and inspect.
4. `mapPartitions` (line 176) runs `processPartitionWithReturn` on executors: for each aggregated
   customer, it validates records (`AggregatorValidator`, active/inactive + CVR + overdue + size
   checks), batches them in groups of 100 (line 372), and `processBatch` (line 222) **writes each
   batch to both Kafka and Cassandra**.
5. The processed notifications are also written back to S3 as a Hudi table for validation
   (`writeProcessedNotificationsToS3`, line 747).
6. `configBroadcast.destroy()` and `spark.stop()` clean up.

This is a complete, realistic **lakehouse** pipeline: streaming + batch ingest into a transactional
table format (Hudi) on cheap object storage (S3), then batch analytics/serving out to Kafka and a
low-latency store (Cassandra).

---

## 7. How to Read a Spark Job Like a Pro

When you open any Spark file in this repo (or elsewhere), scan for these five things **in order**:

1. **Where is the SparkSession created?** → that's the entry/config. (e.g. `SparkUtils.createSparkSession`)
2. **What are the sources?** → `spark.read()...` (batch) or `spark.readStream()...` (streaming).
3. **What are the transformations?** → `select/filter/groupBy/withColumn/map/join/union`. These are
   lazy — just a plan.
4. **Where are the actions?** → `count/show/collect/write/save/foreachPartition/awaitTermination`.
   *This* is where computation actually fires. Count how many actions there are — each is a job.
5. **Where are the shuffles?** → `groupBy/join/repartition/distinct/agg`. These are your performance
   hot-spots.

Then ask the driver-vs-executor question: **"Which of this code runs on the driver, and which runs
inside `mapPartitions`/`foreachPartition` on executors?"** Get that right and you understand the job.

---

## 8. The Dependency Stack (pom.xml decoded)

From `pom.xml`, here's what each key dependency is *for*:

| Dependency | Version | Why it's here |
|---|---|---|
| `spark-core_2.12` | 3.4.1 | The Spark engine: RDDs, scheduler, driver/executor runtime. |
| `spark-sql_2.12` | 3.4.1 | DataFrames/Datasets, Catalyst optimizer, SQL, `functions.*`. |
| `spark-sql-kafka-0-10_2.12` | 3.4.1 | The Kafka **source/sink** for Structured Streaming (`format("kafka")`). |
| `hudi-spark3.5-bundle_2.12` | 0.14.x | Apache Hudi table format (`format("org.apache.hudi")`). |
| `hadoop-aws` | 3.3.4 | The **`s3a://` filesystem** (`S3AFileSystem`) — Hadoop's S3 connector. |
| `hadoop-common` / `hadoop-client` | 3.3.4 | Hadoop core: `FileSystem`, `Path`, `Configuration` (used in `HudiUtils`). |
| `aws-java-sdk-bundle` | 1.11.x | Underlying AWS SDK that `hadoop-aws` calls to reach S3. |
| `kafka-clients` | 3.6.1 | Kafka producer/consumer used *outside* streaming (e.g. `SmsAggregatorJob` writing to Kafka). |
| `java-driver-core` (DataStax) | 4.17.0 | Cassandra client — reads/writes the `combine_notification` table. |
| `mysql-connector-java` | 8.0.33 | JDBC driver for the MySQL enrichment lookups in `BillDataIngestionFromCSV`. |
| `spring-boot-starter*`, `spring-kafka` | 3.x | The service/consumer side (`SpringKafkaApplication`, `SpringDwhEventConsumer`). |
| `vault-java-driver` | 5.1.0 | Fetches secrets (DB creds etc.) at runtime — see `VaultLoader`. |
| `jackson-*` | 2.15.2 | JSON serialization (the Kafka payloads, `from_json` schemas mirror this). |
| `maven-shade-plugin` | 3.4.1 | Builds the **fat/uber JAR** (`*-shaded.jar`) that bundles all deps so `spark-submit` has everything. |

**Why the "shaded" JAR matters:** Spark jobs run on remote executors that don't have your
dependencies. The shade plugin packs *everything* into one JAR you can ship to the cluster. That's
the `target/spark-notification-pipeline-1.0-SNAPSHOT-shaded.jar` referenced in the README's run
commands.

**Version note:** the `pom.xml` pulls `spark 3.4.1` and the `hudi-spark3.5-bundle`. Matching Spark
and Hudi/Scala versions (here Scala `2.12`) is a common real-world headache — mismatches cause
`NoSuchMethodError` at runtime. Always keep the Spark line, the Hudi bundle's Spark suffix, and the
`_2.12` Scala suffix consistent.

---

## 9. Glossary

- **ACID** — Atomicity, Consistency, Isolation, Durability: the guarantees that make writes safe.
  Hudi brings ACID to files on S3.
- **Action** — a Spark operation that triggers execution and returns/writes a result (`count`,
  `show`, `write`, `collect`).
- **AQE (Adaptive Query Execution)** — runtime plan re-optimization using real data stats.
- **Block (HDFS)** — a fixed-size chunk (default 128 MB) of a file, replicated across DataNodes.
- **Broadcast variable** — read-only data shipped once per executor and shared by all its tasks.
- **Catalyst** — Spark SQL's query optimizer that rewrites DataFrame/SQL plans for speed.
- **Checkpoint (streaming)** — saved progress (Kafka offsets + state) enabling exactly-once resume.
- **Cluster Manager** — allocates executors (YARN, Kubernetes, Standalone, or `local`).
- **COW / MOR** — Hudi Copy-on-Write (rewrite files, fast reads) vs Merge-on-Read (append deltas,
  fast writes).
- **DataFrame** — `Dataset<Row>`; a distributed table with a named-column schema.
- **Dataset<T>** — a strongly-typed distributed collection of objects of type `T`.
- **Driver** — the single JVM running your `main()`; builds the plan and schedules tasks.
- **Executor** — a worker JVM that runs tasks on partitions and caches data.
- **FileSystem (Hadoop)** — the pluggable storage interface (`hdfs://`, `file://`, `s3a://`).
- **HDFS** — Hadoop's distributed, replicated file storage (NameNode + DataNodes).
- **Hudi** — a transactional table format over files, adding upserts/deletes/indexes/time-travel.
- **Lazy evaluation** — transformations build a plan; nothing runs until an action.
- **Lineage** — the recipe of transformations; lets Spark recompute lost partitions (fault tolerance).
- **MapReduce** — Hadoop's original (now largely superseded) disk-based compute model.
- **Micro-batch** — the small chunk a streaming query processes each trigger interval.
- **Partition** — a chunk of a dataset processed by one task on one core; the unit of parallelism.
- **Precombine key** — Hudi's tie-breaker: on duplicate record keys, the higher precombine value wins.
- **Record key** — Hudi's unique identity of a row; drives upsert/dedup.
- **RDD** — Resilient Distributed Dataset; the low-level distributed collection under DataFrames.
- **Repartition / Coalesce** — change the number of partitions (`repartition` shuffles, `coalesce`
  only shrinks).
- **Shuffle** — network redistribution of data (for `groupBy`/`join`/`repartition`); the main cost.
- **SparkSession** — the entry point to all Spark functionality (`spark.read()`, config, etc.).
- **Stage** — a set of tasks between two shuffles; a job is split into stages.
- **Task** — the smallest unit of execution; one task per partition per stage.
- **Transformation** — a lazy operation that defines a new dataset (`filter`, `select`, `groupBy`).
- **YARN** — Hadoop's cluster resource manager (ResourceManager + NodeManagers + containers).

---

## 10. Cheat-Sheet (Mental Model)

```
STORAGE (Hadoop idea)                COMPUTE (Spark)
─────────────────────                ────────────────
S3 / HDFS  = where bytes live        SparkSession = your handle to Spark
Hudi       = a smart TABLE on top    DataFrame    = distributed table
             (upserts/deletes/ACID)  Partition    = a chunk = 1 task
FileSystem = pluggable connector     Transformation = lazy plan step
             (s3a://, hdfs://)        Action         = "GO!" (runs the plan)
                                      Shuffle        = data crosses network (slow)
                                      Driver         = brain (1)
                                      Executor       = muscle (many)
                                      Broadcast      = ship config once/executor
                                      persist()      = cache to skip recompute
```

**The three golden rules to remember:**
1. **Nothing runs until an action.** (Lazy evaluation.)
2. **Code in `mapPartitions`/`foreachPartition` runs on executors, in parallel, on machines that are
   not your driver.** Open connections *per partition/executor*, not per row.
3. **Shuffles (`groupBy`, `join`, `repartition`) are the expensive part.** Count them, minimize them.

---

### Where to look next in this repo (a guided reading order)
1. `src/main/java/com/notification/util/SparkUtils.java` — how a session is configured (start here).
2. `src/main/java/com/notification/config/S3Config.java` + `HdfsConfig.java` — Hadoop filesystem wiring.
3. `src/main/java/com/notification/BillDataIngestionFromCSV.java` — the simplest full batch job.
4. `src/main/java/com/notification/util/HudiUtils.java` — read/write Hudi + raw Hadoop `FileSystem` use.
5. `src/main/java/com/notification/SmsAggregatorJob.java` — the richest job: groupBy, persist,
   broadcast, mapPartitions, dual-write.
6. `src/main/java/com/notification/KafkaStreamingHudiWriter.java` — the full Structured Streaming job.
7. `README.md` — the operational/config view of the same system.
8. `pom.xml` — the dependency stack decoded in section 8 above.
