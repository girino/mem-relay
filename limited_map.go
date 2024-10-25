package main

type LimitedMap struct {
	data  map[string]interface{}
	order []string
	limit int
}

func NewLimitedMap(limit int) *LimitedMap {
	return &LimitedMap{
		data:  make(map[string]interface{}),
		order: make([]string, 0, limit),
		limit: limit,
	}
}

func (lm *LimitedMap) Add(key string, value interface{}) {
	if _, exists := lm.data[key]; !exists {
		if len(lm.order) >= lm.limit {
			oldestKey := lm.order[0]
			lm.order = lm.order[1:]
			delete(lm.data, oldestKey)
		}
		lm.order = append(lm.order, key)
	}
	lm.data[key] = value
}

func (lm *LimitedMap) Get(key string) (interface{}, bool) {
	value, exists := lm.data[key]
	return value, exists
}
