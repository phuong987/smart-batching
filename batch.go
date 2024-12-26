package smartbatching

const (
	addOp int = iota
	finishOp
)

type opItem struct {
	kind int
	key  string
	data itemBatch
}
type itemBatch struct {
	rep  chan interface{}
	data interface{}
}

type processBatch interface {
	Do(key string, datas []interface{}) []interface{}
}
type SmartBatching struct {
	p      processBatch
	chanOp chan opItem
}

func (s *SmartBatching) run() {
	batchMap := make(map[string][]itemBatch)
	for op := range s.chanOp {
		key := op.key
		batch, exist := batchMap[key] // batch is a copy of batchMap[key]

		if op.kind == addOp {
			if exist { // that means goroutine processBatch is running and hasn't sent finishOp yet
				batchMap[key] = append(batchMap[key], op.data) // append op.data to []itemBatch which will be processed when receive finishOp signal
				batch = nil                                    // assign slice nil to batch
			} else {
				batchMap[key] = nil          // firstly, []itemBatch is slice nil (not initialized)
				batch = []itemBatch{op.data} // now batch is a slice having 1 element which is a copy of op.data
			}
		} else if op.kind == finishOp {
			if len(batch) == 0 { // no item backlogged during processBatch
				delete(batchMap, key)
			} else { // mark key on processing with batch
				batchMap[key] = nil
			}
		}

		if len(batch) > 0 {
			go s.processBatch(key, batch)
		}
	}
}
func (s *SmartBatching) processBatch(key string, batch []itemBatch) {
	data := make([]interface{}, 0, len(batch))
	for _, item := range batch {
		data = append(data, item.data)
	}

	// Process the batch and return responses
	for i, rep := range s.p.Do(key, data) {
		batch[i].rep <- rep // although batch[i] is a copy of op.data, rep is a pointer (channel) so can send response to original channel of op.data
	}

	// Mark finish operation for the key
	s.chanOp <- opItem{
		kind: finishOp,
		key:  key,
	}
}
func (s *SmartBatching) Add(key string, data interface{}) interface{} {
	rep := make(chan interface{})
	s.chanOp <- opItem{addOp, key, itemBatch{rep, data}}

	return <-rep // wait response from processBatch
}
func NewSmartBatching(p processBatch) *SmartBatching {
	s := &SmartBatching{
		p:      p,
		chanOp: make(chan opItem), // unbuffered channel type opItem for receiving data synchronously & blocking from other goroutines
	}
	go s.run() // wait data sent by other goroutines through channel by using Add function

	return s
}
