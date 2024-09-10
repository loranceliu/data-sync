package signal

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func SignalHandle(shutdown func()) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		// 等待信号
		<-signalCh

		// 停止 BinlogProcessor
		shutdown()
	}()
}
