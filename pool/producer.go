package pool

type Producer struct {
	jobQueue chan Job
}

func NewProducer(queue chan Job) *Producer {
	return &Producer{jobQueue: queue}
}

func (p *Producer) Produce(job Job) {
	p.jobQueue <- job
}
