from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from message_source import MessageSource, Packet


@dataclass
class LoggerStats:
    packets_received: int = 0       # Total packets from source
    packets_written: int = 0        # Packets written to log
    duplicates_discarded: int = 0   # Duplicate packets ignored
    corrupted_packets: int = 0      # Packets with bad checksum
    retransmit_requests: int = 0    # Times request_retransmit called
    retransmits_received: int = 0   # Successful retransmits
    inversions: int = 0             # Out-of-order writes (LATE status)
    gaps: int = 0                   # Missing sequences in final log
    buffer_flushes: int = 0         # Times buffer was flushed
    final_buffer_size: int = 0      # Packets in buffer at termination (lost)


class EventLogger:
    def __init__(self,
                 source: MessageSource,
                 log_file: Path,
                 buffer_size: int = 30):
        """
        Initialize event logger.

        Args:
            source: Message source to receive from
            log_file: Path to append-only log file
            buffer_size: Max packets to buffer before forced flush
        """
        self.source = source
        self.log_file = log_file
        self.buffer_size = buffer_size
        
        # Your state variables
        self.buffer: list[Packet] = []
        self.seen_sequences: set[int] = set()
        self.last_written_seq: int = -1
        self.pending_retransmits: set[int] = set()
        # Add more as needed

        self.expected_sequence: int = 0
        self.retransmitted_seqs: set[int] = set()
        self.gap_wait_cycles: int = 0
        self.gap_wait_limit: int = max(6, buffer_size // 2)
        self.gap_skip_limit: int = max(1, buffer_size // 10)

        self.stats = LoggerStats()

        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_file.exists():
            self.log_file.touch()

        self._load_existing_log()

    def run(self) -> LoggerStats:
        """
        Main processing loop.

        Continuously receive packets until termination.
        Handle each packet appropriately:
        - Verify checksum (request retransmit if corrupted)
        - Detect duplicates (discard if already seen)
        - Buffer or write based on your strategy
        - Periodically flush buffer

        Returns:
            Statistics about logging performance.
        """
        try:
            while True:
                packet = self.source.receive()
                if packet is None:
                    self._finalize()
                    return self.stats

                self.stats.packets_received += 1
                self._handle_packet(packet)

                if self._should_flush():
                    self._flush_buffer()

        except SystemExit:
            self._finalize()
            return self.stats

    def _handle_packet(self, packet: Packet) -> None:
        """Process a single packet."""
        seq_id = packet.sequence

        if self.source.verify_checksum(packet):
            pass
        else:
            self.stats.corrupted_packets += 1
            
            if seq_id in self.pending_retransmits:
                return
            self.source.request_retransmit(seq_id)
            self.pending_retransmits.add(seq_id)
            self.retransmitted_seqs.add(seq_id)
            self.stats.retransmit_requests += 1
                
            return

        if seq_id in self.seen_sequences:
            self.stats.duplicates_discarded += 1
            return

        self.seen_sequences.add(seq_id)

        if seq_id in self.retransmitted_seqs:
            self.stats.retransmits_received += 1

        self.buffer.append(packet)

    def _should_flush(self) -> bool:
        """Determine if buffer should be flushed."""
        if len(self.buffer) >= self.buffer_size:
            return True

        has_expected = any(seq_id.sequence == self.expected_seq for seq_id in self.buffer)

        if has_expected:
            self.gap_wait_cycles = 0
        else:
            self.gap_wait_cycles += 1

        if self.gap_wait_cycles < self.gap_wait_limit:
            return False
        return True

    def _flush_buffer(self) -> None:
        """Write buffered packets to log."""
        if not self.buffer:
            return

        self.stats.buffer_flushes += 1
        self.buffer.sort(key=lambda seq_id: seq_id.sequence)

        while True:
            packet = None
            for i, p in enumerate(self.buffer):
                if p.sequence == self.expected_sequence:
                    packet = self.buffer.pop(i)
                    break

            if packet is None:
                break

            if packet.sequence in self.retransmitted_seqs:
                status = "RETRANSMIT"
            else:
                status = "OK"
            self._write_packet(packet, status=status)
            self.expected_sequence += 1

        self.buffer.sort(key=lambda seq_id: seq_id.sequence)

        if not any(seq_id.sequence == self.expected_sequence for seq_id in self.buffer):
            exp_seq_id = self.expected_sequence

            if exp_seq_id not in self.pending_retransmits:
                self.source.request_retransmit(exp_seq_id)
                self.pending_retransmits.add(exp_seq_id)
                self.retransmitted_seqs.add(exp_seq_id)
                self.stats.retransmit_requests += 1

        skip_count = 0
        while skip_count < self.gap_skip_limit and (self.buffer != []):
            self.buffer.sort(key=lambda seq_id: seq_id.sequence)
            smallest_seq_id = self.buffer[0].sequence

            if smallest_seq_id > self.expected_sequence:
                self.stats.gaps += 1
                self.expected_sequence += 1
                skip_count += 1

                while True:
                    packet = None
                    for i, p in enumerate(self.buffer):
                        if p.sequence == self.expected_sequence:
                            packet = self.buffer.pop(i)
                            break

                    if packet is None:
                        break

                    if packet.sequence in self.retransmitted_seqs:
                        status = "RETRANSMIT"
                    else:
                        status = "OK"
                    
                    self._write_packet(packet, status=status)
                    self.expected_sequence += 1
            else:
                break

        while len(self.buffer) > self.buffer_size:
            self.buffer.sort(key=lambda seq_id: seq_id.sequence)
            packet = self.buffer.pop(0)
            if packet.sequence in self.retransmitted_seqs:
                status = "RETRANSMIT"
            else:
                status = "LATE"
            self._write_packet(packet, status=status)

    def _finalize(self) -> None:
        """Called after termination. Flush remaining buffer."""
        self._flush_buffer()
        self.buffer.sort(key=lambda seq_id: seq_id.sequence)
        while self.buffer != []:
            packet = self.buffer.pop(0)
            if packet.sequence in self.retransmitted_seqs:
                status = "RETRANSMIT"
            else:
                status = "LATE"
            self._write_packet(packet, status=status)

        self.stats.final_buffer_size = 0

    def _write_packet(self, packet: Packet, status: str) -> None:
        seq_id = packet.sequence
        time_log = packet.timestamp
        pl_hex = packet.payload.hex()

        if self.last_written_seq != -1 and seq_id != self.last_written_seq + 1:
            out_of_order = True
        else:
            out_of_order = False

        if status == "OK" and out_of_order:
            status = "LATE"

        if status == "LATE":
            self.stats.inversions += 1
        elif status == "RETRANSMIT" and out_of_order:
            self.stats.inversions += 1

        line = f"{seq_id},{time_log:.6f},{pl_hex},{status}\n"
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(line)

        self.stats.packets_written += 1
        self.last_written_seq = seq_id

    def _load_existing_log(self) -> None:
        try:
            content = self.log_file.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            content = ""

        if not content:
            self.expected_sequence = 0
            self.last_written_seq = -1
            return

        last_seq_id = -1
        for log_infos in content.splitlines():
            info = log_infos.split(",", 3)
            if len(info) != 4:
                continue
            try:
                seq_id = int(info[0])
            except ValueError:
                continue
            self.seen_sequences.add(seq_id)
            last_seq_id = seq_id

        self.last_written_seq = last_seq_id
        self.expected_sequence = last_seq_id + 1
