package amqp_queue

type ExchangeDeclare struct {
	Name       string // name
	Kind       string // type
	Durable    bool   // durable
	AutoDelete bool   // auto-deleted
	Internal   bool   // internal
	noWait     bool   // no-wait
}
