## Approach

This purpose of the problem is to merge two sorted lists held by two workers using file-based message passing. Therefore, I define three phases during the message passing.
1. INIT
Each worker computes its own minimum, maximum, and count After receiving data, the worker enters the next phase.
2. MERGE
Each worker sends its current smallest unprocessed value using the HEAD message. When both workers know each other’s current value, they compare each other. The smaller value is written to the output and the index will be updated.
3. DONE
When both workers have sent END, the worker finish their job.

## Message Types

There are four message types, including RANG, HEAD, and END (each message is a JSON line with fields msg_type and values):

1. RANG
Purpose: Exchange minimum, maximum and count of each worker’s list.
2. HEAD
Purpose: Indicate the worker’s current smallest value, enabling comparison between two worker's values during merging.
3. END
Purpose: Indicate that the worker has no remaining values.
4. TAKE
Purpose: Request the partner to output its current smallest value.

## Merge Strategy

I used a bubble-style merge strategy.
Workers compare their current smallest values and only the global minimum is output at each step.
Therefore, this strategy maintains a balance in the comparison workload between the two workers.

## Work Balance

To balance workload, a bubble-style merge strategy was used, with both workers participating in the merge.
Each worker will sort their list, and they will repeatedly compares its current head value with the other's head value.
When the other's value is smaller, the worker sends a "TAKE" message to move the other forward; otherwise, it moves forward itself.
This strategy distributes the merging work among both workers and prevents one side from bearing the entire merge workload.