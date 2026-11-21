package memory

import "github.com/standards-lab/go-core/event"

func (b *Broker) AppendRaw(typ string, h event.Header, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log = append(b.log, message{typ: typ, header: h, body: body})
	for _, c := range b.consumers {
		c.wakeAll()
	}
}
