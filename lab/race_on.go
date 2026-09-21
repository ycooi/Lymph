//go:build race

package lab

// raceEnabled reports whether the binary was built with the race detector.
// Performance gates are relaxed when it is: instrumentation costs roughly an
// order of magnitude, and a throughput number measured under it says nothing
// about the daemon.
const raceEnabled = true
