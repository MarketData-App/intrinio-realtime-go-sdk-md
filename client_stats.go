package intrinio

import "sync/atomic"

// ClientStats is a point-in-time view of one client's message path.
//
// The counters (DataMessages, BytesReceived, ReadQueueDrops and
// PriorityQueueDrops) only increase for the life of the Client. Stop and
// Start do not reset them, so they can back monotonic metrics. The depths are
// gauges read at the time of the call.
type ClientStats struct {
	// DataMessages counts the binary frames read from the WebSocket.
	DataMessages uint64
	// BytesReceived sums the payload bytes of those frames.
	BytesReceived uint64
	// ReadQueueDepth is the number of frames that wait in the read channel.
	ReadQueueDepth int
	// ReadQueueCapacity is the size of the read channel.
	ReadQueueCapacity int
	// ReadQueueDrops counts the frames dropped because the read channel was full.
	ReadQueueDrops uint64
	// PriorityQueueDepth is the number of messages that wait in the priority queue.
	PriorityQueueDepth int
	// PriorityQueueDrops counts the messages that a full lower-priority queue dropped.
	PriorityQueueDrops uint64
}

// clientCounters holds the monotonic counters of ClientStats.
type clientCounters struct {
	dataMessages   atomic.Uint64
	bytesReceived  atomic.Uint64
	readQueueDrops atomic.Uint64
}

// Stats returns the message-path counters and queue depths of the client. It
// is safe to call from any goroutine, before Start and after Stop.
func (client *Client) Stats() ClientStats {
	stats := ClientStats{
		DataMessages:      client.counters.dataMessages.Load(),
		BytesReceived:     client.counters.bytesReceived.Load(),
		ReadQueueDepth:    len(client.readChannel),
		ReadQueueCapacity: cap(client.readChannel),
		ReadQueueDrops:    client.counters.readQueueDrops.Load(),
	}
	if client.priorityQueue != nil {
		stats.PriorityQueueDepth = client.priorityQueue.depth()
		stats.PriorityQueueDrops = client.priorityQueue.totalDropCount()
	}
	return stats
}
