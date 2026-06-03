package multicast

// Platform-specific socket options for multicast and QoS settings.
// Implementations for SetMulticastTTL and SetDSCP are defined in:
// - socket_opts_windows.go (for Windows platforms)
// - socket_opts_posix.go   (for non-Windows platforms)
