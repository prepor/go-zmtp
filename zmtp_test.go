package zmtp

import (
	"testing"
	"time"

	"github.com/prepor/zmtp/internal/test"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	go test.StartExternalServer()
	time.Sleep(100 * time.Millisecond)
	pingTest(t)
}

func TestListener(t *testing.T) {
	listener, err := Listen(&SocketConfig{
		Type:     REP,
		Endpoint: "127.0.0.1:31337",
	})
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		acc := <-listener.Accept()
		if acc.err != nil {
			t.Error(acc.err)
			return
		}
		read, write := acc.socket.Channels()
		v := <-read
		write <- v
		listener.Close()
	}()
	pingTest(t)
}

func pingTest(t *testing.T) {
	socket, err := Connect(&SocketConfig{
		Type:     REQ,
		Endpoint: "127.0.0.1:31337",
	})
	if err != nil {
		t.Error(err)
		return
	}
	read, write := socket.Channels()
	write <- [][]byte{[]byte(""), []byte("Hello!")}
	response := <-read
	assert.Equal(t, "Hello!", string(response[1]))
	socket.Close()
}
