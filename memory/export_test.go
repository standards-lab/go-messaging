package memory

import "github.com/standards-lab/go-core/event"

func (b *Broker) AppendRaw(subject string, h event.Header, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.add(message{subject: subject, header: h, body: body})
}

func (b *Broker) ForgetDuplicates() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.seen)
}
