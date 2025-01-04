package dispatcher

import (
	"fmt"
	"net/rpc"
	"strings"
)

type Dispatcher struct {
	Client     *rpc.Client
	CommandMap map[string]func([]string)
}

func InitializeDispatcher() *Dispatcher {
	return &Dispatcher{
		CommandMap: make(map[string]func([]string)),
	}
}

func (h *Dispatcher) RegisterCommand(name string, handler func([]string)) {
	h.CommandMap[name] = handler
}

func (h *Dispatcher) Handle(command string) {
	parts := strings.Fields(command)
	if len(parts) < 1 {
		fmt.Println("Invalid command. Please try again.")
		return
	}

	if handlers, exists := h.CommandMap[parts[0]]; exists {
		handlers(parts[1:])
	} else {
		fmt.Println("Unknown command. Available commands: SET, GET, GETALL, DELETE")
	}

}
