## buffer size choice and reasoning
I use a buffer size of 30 packets.
This size is large enough to tolerate moderate packet reordering and temporary gaps.
Also, this size can prevente excessive delay before writing packets to the log.

## flush strategy
Before writing packets, the logger checks for the expected next sequence number. If the expected packet is present in the buffer, it will be written.
The buffer is also flushed when it reaches its maximum capacity.
If a gap persists beyond a waiting threshold, the logger proceeds to flush and skip the missing sequence.
Upon system termination, all remaining buffered packets are flushed.

## gap handling approach
When an expected sequence number is missing, the logger will wait for a limited number of cycles. If the gap still persists, the logger skips the missing sequence.

## trade-offs
A smaller buffer reduces latency but leads to more out-of-order writes.
A larger buffer improves ordering accuracy but increases latency.
Therefor, we need to find the balance.