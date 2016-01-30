#include "czmq.h"

void startExternalServer()
{
  zsock_t *server = zsock_new_rep("tcp://127.0.0.1:31337");
  char *msg = zstr_recv (server);
	zstr_send (server, msg);
  zsock_destroy (&server);
}
