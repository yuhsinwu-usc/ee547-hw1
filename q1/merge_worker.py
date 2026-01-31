import json
from pathlib import Path
from dataclasses import dataclass

@dataclass
class Message:
    msg_type: str   # Max 5 chars
    values: list[int]  # Max 10 integers

@dataclass
class WorkerStats:
    comparisons: int      # Number of comparison operations
    messages_sent: int    # Number of messages written
    messages_received: int # Number of messages read
    values_output: int    # Number of values written to output

class MergeWorker:
    def __init__(self,
                 worker_id: str,        # "A" or "B"
                 data: list[int],       # This worker's sorted data
                 inbox: Path,           # Read messages from here
                 outbox: Path,          # Write messages here
                 output: Path,          # Append merged results here
                 state_file: Path):     # Persist state between steps
        self.worker_id = worker_id
        self.data = sorted(data)
        self.inbox = inbox
        self.outbox = outbox
        self.output = output
        self.state_file = state_file
        self.stats = WorkerStats(0, 0, 0, 0)

        self.state: dict = self._load_state()

    def _load_state(self) -> dict:
        """Load state from file, or initialize if first run."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return self._initial_state()

    def _save_state(self) -> None:
        """Persist state to file."""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)
            
    def _initial_state(self) -> dict:
        """Return initial state structure."""
        # Implement this - define your state variables
        return {
            "phase": "INIT",
            "my_min": min(self.data) if self.data else None,
            "my_max": max(self.data) if self.data else None,
            "my_count": len(self.data),
            "partner_min": None,
            "partner_max": None,
            "partner_count": None,
            "data_index": 0,
            "output_count": 0,
            # Add more state variables as needed
            "rang_sent": False,
            "last_sent": None,
            "file_pos": 0,
            "finish": False,
            "b_current": None,
            "b_end": False,
            "take_wait": False,
        }

    def _read_new_msgs(self) -> list[dict]:
        
        if not self.inbox.exists():
            return []

        file = self.inbox.read_text().splitlines()
        start_pos = self.state["file_pos"]
        new_msgs = file[start_pos:]
        self.state["file_pos"] = len(file)

        msgs: list[dict] = []
        for msg in new_msgs:
            if not msg.strip():
                continue
            msgs.append(json.loads(msg))
            self.stats.messages_received += 1
        return msgs
    
    def _send_msgs(self, msg_type: str, values: list[int]) -> None:
        
        if len(msg_type) > 5:
            raise ValueError("Max 5 chars")
        if len(values) > 10:
            raise ValueError("Max 10 integers")

        msg = {"msg_type": msg_type, "values": values}
        
        with open(self.outbox, "a") as f:
            f.write(json.dumps(msg) + "\n")
        self.stats.messages_sent += 1

    def _output_msg(self, v: int) -> None:
        
        with open(self.output, "a") as f:
            f.write(str(v) + "\n")
        self.stats.values_output += 1

    def step(self) -> bool:
        
        if self.state["finish"]:
            self._save_state()
            return False

        inbox_msgs = self._read_new_msgs()

        for msg in inbox_msgs:
            msg_t = msg.get("msg_type")
            msg_vals = msg.get("values", [])

            if msg_t == "RANG":
                if len(msg_vals) == 3:
                    self.state["partner_min"] = msg_vals[0]
                    self.state["partner_max"] = msg_vals[1]
                    self.state["partner_count"] = msg_vals[2]
            elif msg_t == "HEAD":
                if len(msg_vals) == 1:
                    self.state["b_current"] = msg_vals[0]
                    self.state["b_end"] = False
                    self.state["take_wait"] = False
            elif msg_t == "END":
                self.state["b_current"] = None
                self.state["b_end"] = True
                self.state["take_wait"] = False
            elif msg_t == "TAKE":
                idx = self.state["data_index"]
                
                if idx < len(self.data):
                    val = self.data[idx]
                    self._output_msg(val)
                    self.state["data_index"] += 1

                    if self.state["data_index"] < len(self.data):
                        new_head = self.data[self.state["data_index"]]
                        self._send_msgs("HEAD", [new_head])
                        self.state["last_sent"] = new_head
                        self.state["b_end"] = False
                    else:
                        self._send_msgs("END", [])
                        self.state["last_sent"] = "END"
                else:
                    self._send_msgs("END", [])
                    self.state["last_sent"] = "END"

        phase = self.state["phase"]

        # INIT
        if phase == "INIT":
            
            if not self.state["rang_sent"]:
                
                my_min = self.state["my_min"]
                if my_min is None:
                    my_min = -1

                my_max = self.state["my_max"]
                if my_max is None:
                    my_max = -1

                self._send_msgs("RANG", [my_min, my_max, self.state["my_count"]])
                self.state["rang_sent"] = True
                self._save_state()
                return True

            if self.state["partner_count"] is not None:
                self.state["phase"] = "MERGE"
            self._save_state()
            return True

        # MERGE
        if phase == "MERGE":
            
            data_index = self.state["data_index"]
            
            if data_index >= len(self.data):
                done_stage = True
            else:
                done_stage = False

            if done_stage:
                
                if self.state.get("last_sent") != "END":
                    self._send_msgs("END", [])
                    self.state["last_sent"] = "END"
                    self._save_state()
                    return True

                if self.state.get("b_end", False):
                    self.state["phase"] = "DONE"
                self._save_state()
                return True

            curr_head = self.data[data_index]

            if self.state.get("last_sent") != curr_head:
                self._send_msgs("HEAD", [curr_head])
                self.state["last_sent"] = curr_head
                self._save_state()
                return True

            if self.state["b_end"]:
                self._output_msg(curr_head)
                self.state["data_index"] += 1
                self._save_state()
                return True

            b_current = self.state.get("b_current")
            if b_current is not None:
                
                self.stats.comparisons += 1

                if curr_head < b_current:
                    self._output_msg(curr_head)
                    self.state["data_index"] += 1
                else:
                    if not self.state.get("take_wait", False):
                        self._send_msgs("TAKE", [])
                        self.state["take_wait"] = True
                    self._save_state()
                    return True
                
                self._save_state()
                return True

            self._save_state()
            return True

        # DONE
        if phase == "DONE":
            if self.state["b_end"] and self.state["data_index"] >= len(self.data):
                self.state["finish"] = True
                self._save_state()
                return False

            self._save_state()
            return True

        self._save_state()
        return True

    def get_stats(self) -> WorkerStats:
        """Return statistics about work performed."""
        return self.stats
