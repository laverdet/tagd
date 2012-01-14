#include "libeti_worker.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <boost/bind.hpp>
#include <boost/backtrace.hpp>
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH

using namespace std;
using namespace eti;

struct ev_loop* Worker::my_loop = ev_default_loop(0);

/**
 * Listen on a unix socket. Returns a Worker::Server.
 */
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
 * Private function called by libeio when there data to read() or write() is ok to call.
 */
void Worker::fd_cb(struct ev_loop* loop, struct ev_io* watcher, int revents) {
	if (revents & EV_ERROR) {
		throw runtime_error("EV_ERROR");
	}
	Worker& that = *static_cast<Worker*>(watcher->data);

	if (revents & EV_READ) {
		that.read_cb();
	} else {
		that.write_cb();
	}
}

/**
 * read() is ok to call.
 */
void Worker::read_cb() {

	// Read data from the stream
	char buf[read_size];
	size_t len = recv(fd, buf, read_size, 0);
	if (!len) {
		ev_io_stop(my_loop, &fd_watcher);
		close(fd);
		delete this;
		return;
	}

	// Loop through the read data looking for newline. If newline is found parse that and invoke a
	// handler. Continue until no more newlines are around and then put that data in the buffer.
	char* pos = buf;
	for (size_t ii = 0; ii < len;) {
		if (pos[ii] == '\n') {
			value_t arguments;
			read_buffer.append(pos, ii);
			if (!json_spirit::read(read_buffer, arguments)) {
				cerr <<"invalid payload\n";
				read_buffer.clear();
			} else {
				read_buffer.clear();
				handle_payload(arguments);
			}
			pos += ii + 1;
			len -= ii + 1;
			ii = 0;
			continue;
		} else {
			++ii;
		}
	}
	if (len) {
		read_buffer.append(pos, len);
	}
}

/**
 * send() is ok to call.
 */
void Worker::write_cb() {
	boost::lock_guard<boost::mutex> lock(write_lock);
	while (!write_buffer.empty()) {
		buffer_t& buffer = write_buffer.front();
		size_t wrote = send(fd, buffer.data + buffer.offset, buffer.length, MSG_DONTWAIT);
		if (wrote != buffer.length) {
			buffer.offset += wrote;
			buffer.length -= wrote;
		} else {
			write_buffer.pop_front();
		}
	}
	ev_io_set(&fd_watcher, fd, EV_READ);
}

void Worker::handle_payload(const value_t& value) {
	try {
		const vector<value_t>& payloads = value.get_array();
		foreach (const value_t& payload, payloads) {
			const string& type = payload.get_obj().find("type")->second.get_str();
			const string& name = payload.get_obj().find("name")->second.get_str();
			const vector<value_t>& args = payload.get_obj().find("data")->second.get_array();
			if (type == "request") {
				map<const string, Server::request_handler_t>::iterator ii = server.request_handlers.find(name);
				if (ii == server.request_handlers.end()) {
					throw runtime_error("unknown request received");
				}
				server.threads.schedule(boost::bind(
					Worker::Server::request_wrapper,
					ii->second,
					this,
					payload.get_obj().find("uniq")->second.get_str(),
					args
				));
			} else if (type == "message") {
				map<const string, Server::message_handler_t>::iterator ii = server.message_handlers.find(name);
				if (ii == server.message_handlers.end()) {
					throw runtime_error("unknown message received");
				}
				server.threads.schedule(boost::bind(ii->second, boost::ref(*this), args));
			} else {
				throw runtime_error("unknown payload received");
			}
		}
	} catch (runtime_error const &err) {
		cerr <<err.what() <<"\n";
		cerr <<boost::trace(err) <<"\n";
		cerr <<json_spirit::write(value) <<"\n";
	}
}

void Worker::Server::request_wrapper(request_handler_t fn, Worker* worker, const request_handle_t& handle, const std::vector<value_t>& args) {
	try {
		fn(*worker, handle, args);
	} catch (runtime_error const &err) {
		worker->respond(handle, err.what(), true);
	}
}

void Worker::respond(const request_handle_t& handle, const value_t& value, bool threw) {
	string response(threw ? "[{\"type\":\"threw\",\"uniq\":\"" : "[{\"type\":\"resolved\",\"uniq\":\"");
	response += handle + "\",\"data\":";
	response += json_spirit::write(value);
	response += "}]\n";
	const char* buf = response.c_str();
	{
		boost::lock_guard<boost::mutex> lock(write_lock);
		size_t wrote = 0;
		if (write_buffer.empty()) {
			wrote = send(fd, buf, response.length(), MSG_DONTWAIT);
		}
		if (wrote != response.length()) {
			ev_io_set(&fd_watcher, fd, EV_READ | EV_WRITE);
			write_buffer.push_back(new buffer_t(buf + wrote, response.length() - wrote));
		}
	}
}
