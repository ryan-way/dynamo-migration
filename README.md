# dynamo-migration

## Individual Updates
### 100

| profile  | time (ms) | percent | time per item |
|---|---|---|---|
| 'insert' | 2964.94 | 1 | 29.65
| 'update' | 3297.05 | 0.44 | 32.97
| 'delete' | 3001.93 | 0.45 | 30.02

### 1000

| (index) | profile  | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'insert' | 21673.55  | 1       | 21.67         |
| 1       | 'update' | 21421.81  | 0.51    | 21.42         |
| 1       | 'delete' | 22143.92  | 0.51    | 22.14         |


## Batches
### Items 1000, Scan Size 100

| (index) | profile        | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'batch insert' | 1742.29   | 1       | 1.74          |
| 1       | 'batch update' | 1045.44   | 0.48    | 1.05          |
| 1       | 'batch delete' | 1059.18   | 0.53    | 1.06          |

### Items 10000, Scan Size 100

| (index) | profile        | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'batch insert' | 10152.3   | 1       | 1.02          |
| 1       | 'batch update' | 9741.44   | 0.76    | 0.97          |
| 1       | 'batch delete' | 9855.77   | 0.77    | 0.99          |



## Concurrent batches
### Items 1000, Scan Size 100

| (index) | profile                   | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'concurrent batch insert' | 1036.02   | 1       | 1.04          |
| 2       | 'concurrent batch update' | 526.42    | 0.34    | 0.53          |
| 2       | 'concurrent batch delete' | 384.4     | 0.16    | 0.38          |

### Items 10000, Scan Size 100

| (index) | profile                   | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'concurrent batch insert' | 1797.53   | 1       | 0.18          |
| 2       | 'concurrent batch update' | 1128.37   | 0.3     | 0.11          |
| 2       | 'concurrent batch delete' | 1093.33   | 0.24    | 0.11          |

### Items 10000, Scan Size 100

| (index) | profile                   | time (ms) | percent | time per item |
|---|---|---|---|---|
| 0       | 'concurrent batch insert' | 1797.53   | 1       | 0.18          |
| 2       | 'concurrent batch update' | 1128.37   | 0.3     | 0.11          |
| 2       | 'concurrent batch delete' | 1093.33   | 0.24    | 0.11          |

### Items 10000, Scan Size 100
throughput exceeded :(