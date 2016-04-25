#include "libeti_worker.h"

using namespace std;
using namespace eti;

void req_echo(Worker& worker, const Worker::request_handle_t& handle, const vector<Worker::value_t>& args) {
	worker.respond(handle, args);
}

int main(const int argc, const char* argv[]) {
	if (argc != 2) {
		return 1;
	}
	Worker::Server::ptr server = Worker::listen(argv[1]);
	server->register_handler("echo", req_echo);
	Worker::loop();
	return 0;
}
