package memory

import "github.com/standards-lab/go-core/event"

// AppendRaw appends a message to b's log as given, bypassing Publish's
// encoding, so a test can deliver one that does not decode.
func (b *Broker) AppendRaw(typ string, h event.Header, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log = append(b.log, message{typ: typ, header: h, body: body})
	for _, c := range b.consumers {
		c.wakeAll()
	}
}
