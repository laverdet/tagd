#include "libeti_worker.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH

using namespace std;
using namespace eti;

struct ev_loop* Worker::my_loop = ev_default_loop(0);

/**
 * Private function called by libeio when there's a connection to accept().
 */
void Worker::Server::accept_cb(struct ev_loop* loop, struct ev_io* watcher, int revents) {
	if (EV_ERROR & revents) {
		throw runtime_error("EV_ERROR");
	}
	Worker::Server& that = *static_cast<Worker::Server*>(watcher->data);

	struct sockaddr_un client_addr;
	socklen_t client_len = sizeof(client_addr);
	int client_fd = accept(watcher->fd, (struct sockaddr*)&client_addr, &client_len);
	if (client_fd < 0) {
		throw runtime_error("accept() error");
	}

	new Worker(that, client_fd);
}

/**
 * Private function called by libeio when there data to read().
 */
void Worker::read_cb(struct ev_loop* loop, struct ev_io* watcher, int revents) {
	if (EV_ERROR & revents) {
		throw runtime_error("EV_ERROR");
	}
	Worker& that = *static_cast<Worker*>(watcher->data);

	// Read data from the stream
	char buf[read_size];
	size_t len = recv(watcher->fd, buf, read_size, 0);
	if (!len) {
		ev_io_stop(my_loop, watcher);
		close(watcher->fd);
		delete &that;
		return;
	}

	// Loop through the read data looking for newline. If newline is found parse that and invoke a
	// handler. Continue until no more newlines are around and then put that data in the buffer.
	char* pos = buf;
	for (size_t ii = 0; ii < len;) {
		if (pos[ii] == '\n') {
			value_t arguments;
			that.buffer.append(pos, ii);
			if (!json_spirit::read(that.buffer, arguments)) {
				cerr <<that.buffer.length() <<"\n";
				throw runtime_error("failed to parse json");
			}
			that.buffer.clear();
			that.handle_payload(arguments);
			pos += ii + 1;
			len -= ii + 1;
			ii = 0;
			continue;
		} else {
			++ii;
		}
	}
	if (len) {
		that.buffer.append(pos, len);
	}
}

Worker::Server::ptr Worker::listen(const std::string& path) {
	int fd;
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		throw runtime_error("socket() error");
	}

	struct sockaddr_un addr;
	int addr_len = sizeof(addr);
	bzero(&addr, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, path.c_str());
	unlink(path.c_str());
	if (bind(fd, (struct sockaddr*)&addr, SUN_LEN(&addr))) {
		throw runtime_error("bind() error");
	}
	if (::listen(fd, 5) < 0) {
		throw runtime_error("listen() error");
	}

	return Server::ptr(new Server(fd));
}

void Worker::handle_payload(const value_t& value) {
	const vector<value_t>& payloads = value.get_array();
	foreach (const value_t& payload, payloads) {
		const string& type = payload.get_obj().find("type")->second.get_str();
		const string& name = payload.get_obj().find("name")->second.get_str();
		const vector<value_t>& args = payload.get_obj().find("data")->second.get_array();
		if (type == "request") {
			map<const string, Server::request_handler_t>::iterator ii = server->request_handlers.find(name);
			if (ii == server->request_handlers.end()) {
				throw runtime_error("unknown request received");
			}
			(ii->second)(*this, payload.get_obj().find("uniq")->second.get_str(), args);
		} else if (type == "message") {
			map<const string, Server::message_handler_t>::iterator ii = server->message_handlers.find(name);
			if (ii == server->message_handlers.end()) {
				throw runtime_error("unknown message received");
			}
			(ii->second)(*this, args);
		} else {
			throw runtime_error("unknown payload received");
		}
	}
}

void Worker::respond(const request_handle_t& handle, const value_t& value, bool threw) {
	string response(threw ? "[{\"type\":\"threw\",\"uniq\":\"" : "[{\"type\":\"resolved\",\"uniq\":\"");
	response += handle + "\",\"data\":";
	response += json_spirit::write(value);
	response += "}]\n";
	send(fd, response.c_str(), response.length(), 0); // TODO: make this not block
}
