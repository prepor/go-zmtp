package test

/*
#cgo !windows pkg-config: libczmq libzmq libsodium
extern void startExternalServer();
*/
import "C"
import "os"

func init() {
	if err := os.Setenv("ZSYS_SIGHANDLER", "false"); err != nil {
		panic(err)
	}
}

func StartExternalServer() {
	C.startExternalServer()
}
