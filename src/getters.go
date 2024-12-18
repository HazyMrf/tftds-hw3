package crdt

func (c *CRDT) GetHistory() []Operation {
	return c.history
}

func (c *CRDT) GetTimestamps() map[string]map[string]int {
	return c.timestamps
}

func (c *CRDT) GetValue(key string) string {
	return c.data[key]
}

func (c *CRDT) GetData() map[string]string {
	return c.data
}
