package zmtp

import (
	"testing"
	"time"

	"github.com/prepor/zmtp/internal/test"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	go test.StartExternalServer()
	time.Sleep(100 * time.Millisecond)
	pingTest(t)
}

func TestListener(t *testing.T) {
	pingServer(t)
	pingTest(t)
}

func TestSend(t *testing.T) {
	pingServer(t)
	socket, err := Connect(&SocketConfig{
		Type:     REQ,
		Endpoint: "127.0.0.1:31337",
	})
	require.NoError(t, err)
	read, _ := socket.Channels()
	socket.Send("Hello", [][]byte{[]byte(" "), []byte("World")}, []string{"!", "!"}, 1)
	expect := [][]byte{[]byte("Hello"), []byte(" "), []byte("World"), []byte("!"), []byte("!"), []byte("1")}
	require.Equal(t, expect, <-read)
	socket.Close()
}

func TestCompatibility(t *testing.T) {
	pingServer(t)
	_, err := Connect(&SocketConfig{
		Type:     REP,
		Endpoint: "127.0.0.1:31337",
	})
	require.Error(t, err)
}

func pingTest(t *testing.T) {
	socket, err := Connect(&SocketConfig{
		Type:     REQ,
		Endpoint: "127.0.0.1:31337",
	})
	require.NoError(t, err)

	read, write := socket.Channels()
	write <- [][]byte{[]byte(""), []byte("Hello!")}
	response := <-read
	require.Equal(t, "Hello!", string(response[1]))
	socket.Close()
}

func pingServer(t *testing.T) {
	listener, err := Listen(&SocketConfig{
		Type:     REP,
		Endpoint: "127.0.0.1:31337",
	})
	require.NoError(t, err)

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
}
