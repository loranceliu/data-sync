package core

type EventHandler interface {
	Handle(result *ResultJson)
}
