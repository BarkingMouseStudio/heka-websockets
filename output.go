package heka_websockets

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"net/http"
)

type connection struct {
	ws   *websocket.Conn
	send chan []byte
}

type WebSocketsOutputConfig struct {
	Address string `toml:"address"`
}

type WebSocketsOutput struct {
	conf        *WebSocketsOutputConfig
	connections map[*connection]struct{}
	register    chan *connection
	unregister  chan *connection
	broadcast   chan []byte
}

func (wso *WebSocketsOutput) ConfigStruct() interface{} {
	return &WebSocketsOutputConfig{":5000"}
}

func (wso *WebSocketsOutput) Init(config interface{}) (err error) {
	wso.conf = config.(*WebSocketsOutputConfig)
	wso.connections = make(map[*connection]struct{})
	wso.register = make(chan *connection)
	wso.unregister = make(chan *connection)
	wso.broadcast = make(chan []byte, 256)

	// Connections handler
	go func() {
		var conn *connection
		var b []byte
		for {
			select {
			case conn = <-wso.register:
				wso.connections[conn] = struct{}{}
			case conn = <-wso.unregister:
				delete(wso.connections, conn)
				close(conn.send)
			case b = <-wso.broadcast:
				for conn = range wso.connections {
					select {
					case conn.send <- b:
					default:
						delete(wso.connections, conn)
						close(conn.send)
						go conn.ws.Close()
					}
				}
			}
		}
	}()

	// Websocket server and connection handler
	http.Handle("/hekaout", websocket.Handler(func(ws *websocket.Conn) {
		c := &connection{ws, make(chan []byte, 256)}

		wso.register <- c

		defer func() {
			wso.unregister <- c
		}()

		var err error
		for b := range c.send {
			if err = websocket.Message.Send(ws, b); err != nil {
				fmt.Errorf(err.Error())
				break
			}
		}
	}))

	go func() {
		if err := http.ListenAndServe(wso.conf.Address, nil); err != nil {
			fmt.Errorf(err.Error())
		}
	}()

	return nil
}

func (wso *WebSocketsOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	for pc := range or.InChan() {
		wso.broadcast <- pc.Pack.MsgBytes
		pc.Pack.Recycle()
	}
	return nil
}

func init() {
	pipeline.RegisterPlugin("WebSocketsOutput", func() interface{} {
		return new(WebSocketsOutput)
	})
}
