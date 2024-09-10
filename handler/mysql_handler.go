package handler

import (
	"data-sync/core"
	"fmt"
)

type MysqlHandler struct {
}

func (h *MysqlHandler) Handle(result *core.ResultJson) {
	// Implement the logic to handle the event and send data to Elasticsearch
	fmt.Printf("MysqlSearchHandler: %+v\n", result)
	// Add your Elasticsearch handling logic here
}
