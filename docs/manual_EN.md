<p align="center">
  <img src="app_icon.png" alt="Multicast-Bridge Icon" width="120px">
</p>

# Multicast-Bridge Integrated Manual (v0.9.0)

This document is the official English integrated manual for `multicast-bridge`, combining the technical specifications, user manual, and operational notes into a single file.

---

## 1. Technical Specification (System Specification)

### 1-1. System Overview
`multicast-bridge` is a high-performance, secure UDP Multicast tunneling and forwarding tool written in Go. It enables forwarding UDP multicast streams across different networks/locations using a reliable, secure unicast bridge.
- **Sender**: Captures local UDP multicast packets from a specific network interface, encapsulates them with a custom protocol header (applies encryption and FEC redundancy if configured), and forwards them to registered receivers via unicast.
- **Receiver**: Accepts encapsulated unicast packets from the sender, decrypts them (recovers dropped packets using FEC if configured), and reconstructs/re-emits them back as multicast at the target site's network interface.

### 1-2. Protocol & Encapsulation Format
The custom encapsulation header is a fixed **17-byte** block structured as follows:
```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Version    |                 Sequence Number               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                       Unix Timestamp                          +
|                       (Nanoseconds)                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Payload Length       |           FEC Info            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```
- **Version (1 byte)**: Upper 4 bits for Major version (currently `0`), lower 4 bits for Minor version (currently `8`).
- **Sequence Number (4 bytes)**: Monotonically increasing counter for sequence verification and loss detection.
- **Unix Timestamp (8 bytes)**: Nanoseconds Unix timestamp recorded when the sender captured the packet. Used for high-precision one-way latency calculations.
- **Payload Length (2 bytes)**: Original packet (payload) size in bytes.
- **FEC Info (2 bytes)**: Control flags and indices for FEC.
  - Bit 15: FEC packet flag (`0`: Data packet, `1`: Redundant parity packet)
  - Bit 14-8: FEC group number (0 to 127)
  - Bit 7-0: Index within the FEC group (0 to N-1)

### 1-3. Security & Reliability Design (Encrypt-then-FEC)
To achieve high performance, security, and error-correction capability, `multicast-bridge` utilizes an **Encrypt-then-FEC** architecture:
1. **Dynamic Session Key Derivation (PBKDF2)**: A temporary key is dynamically derived using PBKDF2-SHA256 stretching, utilizing the shared passphrase and random challenge bytes (Salt) exchanged during handshake.
2. **Strong AES-GCM Encryption**: The entire payload and encapsulation header are encrypted using AES-GCM (128-bit) to guarantee confidentiality and integrity (AEAD).
3. **Reed-Solomon FEC Redundancy**: FEC encoding (parameters `k`, `n`) is applied **after** the encryption step. This allows the receiver to perform loss reconstruction **before** decryption, eliminating cryptographic overhead for missing packets and recovering streams cleanly.
4. **Challenge-Response Mutual Authentication**: Exchanged during the initial registration process to verify the shared passphrase and check clock drift between nodes using cryptographically secure handshakes.

---

## 2. Operation Manual

### 2-1. Build Instructions
Requires Go 1.21 or later installed.
```bash
# Build the binary
go build -o multicast-bridge main.go
```

### 2-2. Configuration Files YAML Setup

> **⚠️ Security Notice**: Configuration files may contain a plaintext passphrase. **Do not commit them to a public repository.** Template files (`*.yaml.example`) are provided in the `config/` directory — copy them and edit locally:
> ```bash
> cp config/send.yaml.example config/send.yaml
> cp config/recv.yaml.example config/recv.yaml
> ```

#### Sender Configuration (`send.yaml`)
```yaml
multicast:
  address: "239.0.0.1"      # Multicast Group Address to capture
  port: 5004                # Multicast Port
  interface: "eth0"         # Capture interface name or local IP

unicast:
  control_port: 5100        # Listening control plane port for receiver connections
  data_port: 5101           # Unicast data transmission port
  max_sessions: 8           # Maximum allowed concurrent receiver sessions

keepalive:
  interval: 1               # Keep-alive transmission and monitor interval in seconds

fec:
  enabled: true             # Enable Reed-Solomon FEC
  k: 8                      # Number of data blocks
  n: 10                     # Total blocks (Parity blocks = n - k = 2)

encryption:
  enabled: true             # Enable encryption & challenge-response auth
  passphrase: "your-passphrase-here" # ⚠️ Stored as plaintext. Do not commit this file.

log:
  level: "INFO"             # DEBUG, INFO, WARN, ERROR
  file: "send.log"          # File path for logs

stats_interval: 10          # Periodic stats dump interval in seconds (Windows only)
```

#### Receiver Configuration (`recv.yaml`)
```yaml
sender:
  address: "192.168.1.100"  # Unicast IP address of the Sender
  control_port: 5100        # Control port of the Sender
  data_port: 5101           # Local port to bind and receive unicast data from sender

multicast:
  address: "239.0.0.1"      # Target Multicast address to re-emit
  port: 5004                # Target Multicast Port
  interface: "eth1"         # Re-emit output network interface name or IP

keepalive:
  interval: 1               # Keep-alive transmission interval in seconds

fec:
  enabled: true             # Enable FEC decoding and reconstruction
  test: false               # Enable passive FEC simulation (FEC Analyzer test mode)

encryption:
  enabled: true             # Enable GCM decryption
  passphrase: "your-passphrase-here" # ⚠️ Stored as plaintext. Do not commit this file.

log:
  level: "INFO"
  file: "recv.log"

stats_interval: 10          # Periodic stats dump interval in seconds (Windows only)
```

### 2-3. Quick Start via Command Line Flags
You can override options and quickly start the tool using command-line arguments.

- **Launch Sender**:
  ```bash
  ./multicast-bridge send --multicast 239.0.0.1:5004 --interface 192.168.1.100 --stats-interval 5
  ```
- **Launch Receiver**:
  ```bash
  ./multicast-bridge recv --sender 192.168.1.100:5100 --multicast 239.0.0.1:5004 --interface 10.0.0.5 --stats-interval 5 --fec-test
  ```
- **Local Loopback Test Mode**:
  Enables running a bridging simulation locally on a single machine:
  ```bash
  # Terminal 1 (Receiver)
  ./multicast-bridge recv --loopback --sender 127.0.0.1:5100 --multicast 239.0.0.1:5004
  # Terminal 2 (Sender)
  ./multicast-bridge send --loopback --target 127.0.0.1:5101 --interface 127.0.0.1
  ```

### 2-4. Monitoring & Statistics Dumping

#### On Windows Environments
Windows builds periodically print diagnostic metrics to logs according to the `stats_interval` setting (defaults to 10 seconds):
* **Sender Metrics Example**:
  ```text
  === Sender Statistics ===
  Throughput: 145.20 Kbps
  Active Sessions (1):
    - 192.168.1.200:5101 (Joined: 09:30:12, LastActive: 09:30:45)
  Timeout History (0):
  =========================
  ```
* **Receiver Metrics Example**:
  ```text
  === Receiver Statistics ===
  Throughput: 145.20 Kbps
  Packet Loss Rate: 0.12%
  Latency:
    Overall - Min: 2.1ms, Max: 12.5ms, Avg: 4.8ms
    Recent 100 - Min: 2.3ms, Max: 6.8ms, Avg: 3.9ms
  =========================

  === FEC Optimization Analysis Matrix ===
  FEC (k, n)   Overhead %   Recovery Rate   Final Loss %   Verdict
  -----------------------------------------------------------------
  (8 , 9 )      12.5      %   82.40        %   0.4320      %   POOR (Loss remains)
  (8 , 10)      25.0      %   99.98        %   0.0005      %   GOOD (Near 100% Recovery)
  (16, 20)      25.0      %   100.00       %   0.0000      %   EXCELLENT (100% Recovered)
  (4 , 6 )      50.0      %   100.00       %   0.0000      %   EXCELLENT (100% Recovered)
  -----------------------------------------------------------------
  Recommended Parameters: FEC (k=16, n=20) at 25.0% overhead
  ========================================
  ```

#### On Linux Environments
Linux builds support the periodic dump timer, as well as **on-demand signal-driven statistics logging**. Sending a `SIGUSR1` signal to the daemon triggers an **immediate** full diagnostic report outputted directly into the log file:
```bash
# Trigger immediate report dump using PID file
kill -USR1 $(cat /var/run/multicast-bridge.pid)
```

---

## 3. Important Notices (Operations & Tuning)

### 3-1. Packet Size Limitation (MTU Limit: 1438 bytes)
To avoid IP fragmentations and severe UDP overhead over standard Ethernet connections, the maximum allowable original multicast packet size is capped at **1438 bytes**.
- Any captured packet exceeding 1438 bytes will log a warning `[301]` and be safely discarded.
- Ensure that sending devices' MTUs or application payload sizes are optimized to fit within this limit.

### 3-2. NTP/PTP Time Synchronization Requirement (H7)
Since key exchanges, Challenge-Response mutual authentication, latency calculations, and time-drift verification rely on nanoseconds Unix timestamps, high-accuracy clocks are mandatory.
- Clocks drifting more than **100ms** between sender and receiver trigger warning logs `[105]`.
- Severe clock drifts can cause authentication handshake failures. Ensure both machines synchronize with a high-accuracy NTP daemon (e.g., `chronyd`) or PTP (Precision Time Protocol, e.g., `ptp4l`). For real-time, low-latency streams (e.g., broadcast video), PTP synchronization targeting sub-microsecond precision is highly recommended.

### 3-3. FEC Parameter Tuning
FEC parameters `(k, n)` must be tailored based on the network's packet loss rate.
- Increasing redundancy (larger `n - k` parity packets) protects against bursty losses but introduces bandwidth overhead and minor processing delays.
- **Recommended Setup**: For typical wide-area corporate networks (losses < 1%), `k=8, n=10` or `k=16, n=20` is optimal. For highly lossy environments like mobile wireless tunnels, highly redundant schemes like `k=4, n=6` are recommended.
- **Passive Evaluation Mode (FEC Analyzer)**:
  `multicast-bridge` includes a built-in **"Passive FEC Simulator (FEC Analyzer)"** that automatically computes the optimal `(k, n)` values without introducing any artificial packets or bandwidth overhead.
  Launching the receiver with the `--fec-test` flag (or `fec.test: true` in the configuration) triggers real-time simulation across multiple representative FEC setups, dumping the recovery matrix directly into the statistics logs. Operators can easily configure the best setting using this concrete data evidence.

### 3-4. OS Kernel Socket Buffers (H6)
When streaming high bandwidths (tens of Mbps or high-framerate media), the default OS socket buffers can become a bottleneck, leading to packets dropped at the socket layer.
- `multicast-bridge` automatically attempts to expand socket buffers to the configured size (default **2MB** / adjustable via `socket_buffer_size` in the config).
- If the requested size exceeds the OS limits, startup verification (`[Startup Check] Step 6/10`) logs a `[403]` error and exits.
- To resolve this, increase system-wide max socket buffer limits (recommended: 8MB or higher):
  ```bash
  # On Linux: Append to /etc/sysctl.conf and apply (sysctl -p)
  net.core.rmem_max=8388608
  net.core.wmem_max=8388608
  ```
  ```powershell
  # On Windows (Administrator):
  # Dynamic allocation handles this by default, but you may need to tune network interface adapter buffers 
  # or adjust AFD.sys buffer limit parameters in the registry under high-stress environments.
  ```

---

## 4. Startup Checklist & Diagnostic Codes

Upon startup, `multicast-bridge` executes a strict **10-step checklist**. If any step fails, it prints a diagnosis code and exits.

### 4-1. 10-Step Startup Checklist Flow

| Step | Validation Target | Failure Code | Diagnostics & Solution |
| :--- | :--- | :---: | :--- |
| **1** | Configuration Availability | `[201]` | Specified YAML configuration file cannot be found. Verify paths. |
| **2** | Value Validation | `[202]` | Invalid parameter values in YAML (e.g., negative ports, invalid IPs). Check logs. |
| **3** | Network Interface Check | `[401]` | Network interface specified is down or not found. Check interface configurations. |
| **4** | Port Availability Check | `[203]` | Configured ports are already bound by another process. Free ports or change configurations. |
| **5** | IGMP Join / Channel Creation | `[401]` / `[403]` | Failed to join multicast group or create outbound Dial sockets. Check network routing. |
| **6** | Socket & Buffer Setup | `[403]` | Failed to allocate large socket buffers. Check max kernel parameters. |
| **7** | Clock Synchronization Check | `[105]` (Warn) | Clock offset between nodes exceeds 100ms. Check NTP synchronization status. |
| **8** | Protocol Version Verification | `[102]` / `[106]` | Major mismatch (`[102]`) terminates connection. Minor mismatch (`[106]`) logs warning. Keep binary versions aligned. |
| **9** | Challenge-Response Auth | `[101]` | Handshake authentication failed due to invalid passphrases or tampering. Check `passphrase`. |
| **10** | Forwarding Core Init | - | Forwarding engine starts successfully. Stream routing commences. |

### 4-2. Runtime Diagnostic & Warning Codes
- **`[104]` (Fatal/Error)**: Sender reached concurrent session limits. Connection rejected for new receiver. Increase `max_sessions` in `send.yaml`.
- **`[301]` (Warning)**: Inbound packet exceeds 1438-byte MTU limits and is discarded.
- **`[302]` (Warning)**: KeepAlive packets missing. Receiver session timed out and is removed from active forwarders.
- **`[304]` (Warning)**: Packet loss gap detected in sequence numbers. Consider increasing FEC parameters.
