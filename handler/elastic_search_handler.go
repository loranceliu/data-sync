package handler

import (
	"data-sync/core"
	"fmt"
)

var i = 1

type ElasticSearchHandler struct {
}

func (h *ElasticSearchHandler) Handle(result *core.ResultJson) {
	fmt.Printf("ElasticSearchHandler:%v\n", result)
}
