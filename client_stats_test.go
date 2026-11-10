package intrinio

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// frameServer sends the same binary frames and one text frame on each
// connection, then keeps the connection open until the client closes it.
func frameServer(t *testing.T, frames [][]byte) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth", authHandler)
	mux.HandleFunc("/socket/websocket", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
			return
		}
		for _, frame := range frames {
			if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestStatsCountFramesBytesAndReadQueueDrops(t *testing.T) {
	// No worker reads the 16-slot read channel, so frames 17 to 20 are dropped.
	frames := make([][]byte, 20)
	wantBytes := uint64(0)
	for i := range frames {
		frames[i] = make([]byte, i+1)
		wantBytes += uint64(i + 1)
	}
	server := frameServer(t, frames)

	client := newConnectionTestClient(server.URL)
	if got := client.Stats(); got != (ClientStats{ReadQueueCapacity: 16}) {
		t.Fatalf("stats before Start = %+v", got)
	}

	client.Start()
	waitForCondition(t, 2*time.Second, func() bool {
		return client.Stats().DataMessages == 20
	}, "20 data messages")

	want := ClientStats{
		DataMessages:      20,
		BytesReceived:     wantBytes,
		ReadQueueDepth:    16,
		ReadQueueCapacity: 16,
		ReadQueueDrops:    4,
	}
	if got := client.Stats(); got != want {
		t.Fatalf("stats while running = %+v, want %+v", got, want)
	}

	// Stop empties the read channel but keeps the counters.
	client.Stop()
	want.ReadQueueDepth = 0
	if got := client.Stats(); got != want {
		t.Fatalf("stats after Stop = %+v, want %+v", got, want)
	}

	// A new session adds to the counters of the old one.
	client.Start()
	waitForCondition(t, 2*time.Second, func() bool {
		return client.Stats().DataMessages == 40
	}, "40 data messages")
	want = ClientStats{
		DataMessages:      40,
		BytesReceived:     2 * wantBytes,
		ReadQueueDepth:    16,
		ReadQueueCapacity: 16,
		ReadQueueDrops:    8,
	}
	if got := client.Stats(); got != want {
		t.Fatalf("stats after restart = %+v, want %+v", got, want)
	}
	client.Stop()
}

func TestStatsReportPriorityQueueDepthAndDropsAcrossReset(t *testing.T) {
	client := newClient(Config{}, 0, 4)
	client.priorityQueue = newPriorityQueue(2, 2)

	// Priority 1 drops its oldest message when full: 5 messages in 2 slots.
	for i := 0; i < 5; i++ {
		client.priorityQueue.enqueue(1, []byte{byte(i)})
	}
	client.priorityQueue.enqueue(0, []byte{9})

	got := client.Stats()
	if got.PriorityQueueDepth != 3 || got.PriorityQueueDrops != 3 {
		t.Fatalf("priority stats = depth %d drops %d, want depth 3 drops 3",
			got.PriorityQueueDepth, got.PriorityQueueDrops)
	}

	// drainSessionBuffers runs on Start and Stop. It clears the per-session
	// drop count that LogStats prints, but not the total.
	client.drainSessionBuffers()
	if drops := client.priorityQueue.dropCount(); drops != 0 {
		t.Fatalf("session drop count after reset = %d, want 0", drops)
	}
	got = client.Stats()
	if got.PriorityQueueDepth != 0 || got.PriorityQueueDrops != 3 {
		t.Fatalf("priority stats after reset = depth %d drops %d, want depth 0 drops 3",
			got.PriorityQueueDepth, got.PriorityQueueDrops)
	}

	client.priorityQueue.enqueue(1, []byte{1})
	client.priorityQueue.enqueue(1, []byte{2})
	client.priorityQueue.enqueue(1, []byte{3})
	if drops := client.Stats().PriorityQueueDrops; drops != 4 {
		t.Fatalf("priority drops after more drops = %d, want 4", drops)
	}
}

func TestEquitiesClientStatsReadTheSessionQueues(t *testing.T) {
	client := NewEquitiesClient(Config{}, nil, nil, nil)
	got := client.Stats()
	if got.ReadQueueCapacity != MAX_EQUITIES_QUEUE_DEPTH {
		t.Fatalf("read queue capacity = %d, want %d", got.ReadQueueCapacity, MAX_EQUITIES_QUEUE_DEPTH)
	}
	client.readChannel <- []byte{1}
	client.priorityQueue.enqueue(0, []byte{1})
	got = client.Stats()
	if got.ReadQueueDepth != 1 || got.PriorityQueueDepth != 1 {
		t.Fatalf("depths = read %d priority %d, want 1 and 1", got.ReadQueueDepth, got.PriorityQueueDepth)
	}
}
