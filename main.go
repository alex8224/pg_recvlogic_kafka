package main

import "C"
import (
	"broker"
	"context"
	"fmt"
	"time"
	"unsafe"
)

func main() {}

var (
	started               = false
	msgbuff               = make(chan []byte, 1000)
	cancelCtx, cancelFunc = context.WithCancel(context.Background())
)

//export ConnectToBroker
func ConnectToBroker(uri *C.char) {
	url := C.GoString(uri)
	fmt.Printf("broker url is %s\n", url)
	broker.NewKafkaConnection(url)
}

//export BridgeToBroker
func BridgeToBroker(rowByteArray unsafe.Pointer, len C.int) {
	rowdata := C.GoBytes(rowByteArray, len)
	if !started {
		started = true
		go broker.SendBytesTo(cancelCtx, msgbuff)
	}
	msgbuff <- rowdata
}

//export ShutDownBroker
func ShutDownBroker() {
	cancelFunc()
	fmt.Println("shutdown broker ......")
	time.Sleep(3 * time.Second)
}
