// Package nodeexec reconciles commands and binary installation on Kubernetes nodes.
// Callers own authorization, maintenance, host-side recovery, and result semantics.
// Pending with reason DispatchClaimUnresolved retains an ambiguous launch claim;
// no instance retries its release or advances its receipt. The exact Job may still
// start from an in-flight request. Retain the receipt and any queued uncertainty
// until that launch is observed or its external outcome is manually resolved.
package nodeexec
