package heka_websockets

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"net/http"
)

type WebSocketsInputConfig struct {
	Address string `toml:"address"`
	Decoder string `toml:"decoder"`
}

type WebSocketsInput struct {
	data chan []byte
	conf *WebSocketsInputConfig
}

func (wsi *WebSocketsInput) ConfigStruct() interface{} {
	return &WebSocketsInputConfig{":4000", "JsonDecoder"}
}

func (wsi *WebSocketsInput) Init(config interface{}) error {
	wsi.conf = config.(*WebSocketsInputConfig)
	wsi.data = make(chan []byte, 256)

	http.Handle("/hekain", websocket.Handler(func(ws *websocket.Conn) {
		var err error
		for {
			var b []byte
			if err = websocket.Message.Receive(ws, &b); err != nil {
				fmt.Println("Websocket:", err.Error())
				break
			}
			wsi.data <- b
		}
	}))

	go func() {
		if err := http.ListenAndServe(wsi.conf.Address, nil); err != nil {
			fmt.Println("Http:", err.Error())
		}
	}()

	return nil
}

func (wsi *WebSocketsInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	// Get the InputRunner's chan to receive empty PipelinePacks
	packs := ir.InChan()

	var decoding chan<- *pipeline.PipelinePack
	if wsi.conf.Decoder != "" {
		// Fetch specified decoder
		decoder, ok := h.DecoderSet().ByName(wsi.conf.Decoder)
		if !ok {
			err := fmt.Errorf("Could not find decoder", wsi.conf.Decoder)
			return err
		}

		// Get the decoder's receiving chan
		decoding = decoder.InChan()
	}

	var pack *pipeline.PipelinePack
	var count int

	// Read data from websocket broadcast chan
	for b := range wsi.data {
		// Grab an empty PipelinePack from the InputRunner
		pack = <-packs

		// Trim the excess empty bytes
		count = len(b)
		pack.MsgBytes = pack.MsgBytes[:count]

		// Copy ws bytes into pack's bytes
		copy(pack.MsgBytes, b)

		if decoding != nil {
			// Send pack onto decoder
			decoding <- pack
		} else {
			// Send pack into Heka pipeline
			ir.Inject(pack)
		}
	}

	return nil
}

func (wsi *WebSocketsInput) Stop() {
	close(wsi.data)
}

func init() {
	pipeline.RegisterPlugin("WebSocketsInput", func() interface{} {
		return new(WebSocketsInput)
	})
}
