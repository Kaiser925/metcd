package raftnode

import "sync"

// Notifier 是一个线程安全的通知器, 可以用于向多个消费者发送关于某些事件的通知
type Notifier struct {
	mu      sync.RWMutex
	channel chan struct{}
}

// NewNotifier returns new notifier
func NewNotifier() *Notifier {
	return &Notifier{
		channel: make(chan struct{}),
	}
}

// Receive 返回一个 channel, 消费者会在通道被关闭时收到通知
func (n *Notifier) Receive() <-chan struct{} {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.channel
}

// Notify 关闭已经传递给消费者的 channel, 并创建新的 channel 用于下一次通知
func (n *Notifier) Notify() {
	newChannel := make(chan struct{})
	n.mu.Lock()
	channelToClose := n.channel
	n.channel = newChannel
	n.mu.Unlock()
	close(channelToClose)
}

// ErrorNotifier 是一个一次性通知器, 用于通知消费者,
// 不会阻塞, 但是只能通知一次, 通知后就会关闭通道
type ErrorNotifier struct {
	c   chan struct{}
	err error
}

func NewErrorNotifier() *ErrorNotifier {
	return &ErrorNotifier{
		c: make(chan struct{}),
	}
}

func (nc *ErrorNotifier) Receive() <-chan struct{} {
	return nc.c
}

func (nc *ErrorNotifier) Notify(err error) {
	nc.err = err
	close(nc.c)
}

func (nc *ErrorNotifier) Error() error {
	return nc.err
}
