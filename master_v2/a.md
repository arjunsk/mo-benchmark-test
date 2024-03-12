| # | Type                    | Insert duration (10_000 rows) | Query QPS (1_000 queries) |
|---|-------------------------|-------------------------------|---------------------------|
| 1 | Without Index           | 7.2sec                        | 1437                      |
| 2 | With 99 Secondary Index | 48sec                         | 322                       |
| 3 | With 1 Master Index     | 14sec                         | 274                       |


```sql

-- no index
mysql> explain analyze SELECT tbl.a100  FROM tbl  WHERE tbl.a65 = 'bh49tOecxt';
+---------------------------------------------------------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                                                                  |
+---------------------------------------------------------------------------------------------------------------------------------------------+
| Project                                                                                                                                     |
|   Analyze: timeConsumed=0ms waitTime=2ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                   |
|   ->  Table Scan on a.tbl                                                                                                                   |
|         Analyze: timeConsumed=1ms waitTime=0ms inputRows=10000 outputRows=1 InputSize=480000bytes OutputSize=24bytes MemorySize=490024bytes |
|         Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                               |
|         Block Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                         |
+---------------------------------------------------------------------------------------------------------------------------------------------+

-- regular secondary index
mysql> explain analyze SELECT tbl.a100  FROM tbl  WHERE tbl.a65 = 'bh49tOecxt';
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                                                                        |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| Project                                                                                                                                           |
|   Analyze: timeConsumed=0ms waitTime=2ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                         |
|   ->  Join                                                                                                                                        |
|         Analyze: timeConsumed=0ms waitTime=6ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                   |
|         Join Type: INDEX                                                                                                                          |
|         Join Cond: (tbl.a100 = __mo_index_secondary_018e31fe-5862-7fc2-a19e-b95a35dbcc30.__mo_index_pri_col)                                      |
|         Runtime Filter Build: #[-1,0]                                                                                                             |
|         ->  Table Scan on a.tbl                                                                                                                   |
|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=48bytes OutputSize=24bytes MemorySize=74bytes             |
|               Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                               |
|               Block Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                         |
|               Runtime Filter Probe: tbl.a100                                                                                                      |
|         ->  Table Scan on a.__mo_index_secondary_018e31fe-5862-7fc2-a19e-b95a35dbcc30                                                             |
|               Analyze: timeConsumed=2ms waitTime=0ms inputRows=10000 outputRows=1 InputSize=740000bytes OutputSize=24bytes MemorySize=750024bytes |
|               Filter Cond: prefix_eq(__mo_index_secondary_018e31fe-5862-7fc2-a19e-b95a35dbcc30.__mo_index_idx_col, 'Fbh49tOecxt ')               |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
15 rows in set (0.04 sec)


-- master index
mysql> explain analyze SELECT tbl.a100  FROM tbl  WHERE tbl.a65 = 'bh49tOecxt';
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                                                                                                                                                               |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Project                                                                                                                                                                                                                                  |
|   Analyze: timeConsumed=0ms waitTime=16ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                                                                                                               |
|   ->  Join                                                                                                                                                                                                                               |
|         Analyze: timeConsumed=0ms waitTime=48ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                                                                                                         |
|         Join Type: INDEX                                                                                                                                                                                                                 |
|         Join Cond: (tbl.a100 = #[1,0])                                                                                                                                                                                                   |
|         Runtime Filter Build: #[-1,0]                                                                                                                                                                                                    |
|         ->  Table Scan on a.tbl                                                                                                                                                                                                          |
|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=48bytes OutputSize=24bytes MemorySize=74bytes                                                                                                    |
|               Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                                                                                                                      |
|               Block Filter Cond: (tbl.a65 = 'bh49tOecxt')                                                                                                                                                                                |
|               Runtime Filter Probe: tbl.a100                                                                                                                                                                                             |
|         ->  Project                                                                                                                                                                                                                      |
|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes                                                                                                    |
|               ->  Table Scan on a.__mo_index_secondary_018e31fe-d88b-75db-89e0-3013ffea73fc                                                                                                                                              |
|                     Analyze: timeConsumed=74ms scan_time=[total=66ms,min=5ms,max=13ms,dop=8] filter_time=[total=7ms,min=0ms,max=1ms,dop=8] waitTime=15ms inputRows=990000 outputRows=1 InputSize=75mb OutputSize=24bytes MemorySize=76mb |
|                     Filter Cond: prefix_eq(#[0,0], 'Fa65 Fbh49tOecxt ')                                                                                                                                                                |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
17 rows in set (0.02 sec)
```