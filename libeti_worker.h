#include <string>
#include <string.h>
#include <memory>
#include <ev.h>
#include <json_spirit.h>

namespace eti {

class Worker {
	public:
		class Server;
		typedef json_spirit::mValue value_t;
		typedef std::string request_handle_t;

	private:
		static const size_t read_size = 4096;
		std::string buffer;

		static struct ev_loop* my_loop;
		Server* server;
		int fd;
		struct ev_io read_watcher;

		static void read_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);
		void handle_payload(const value_t& value);

		/**
		 * Private constructer called by Server.
		 */
		Worker(Server& server, int fd) : server(&server), fd(fd) {
			ev_io_init(&read_watcher, read_cb, fd, EV_READ);
			read_watcher.data = this;
			ev_io_start(Worker::my_loop, &read_watcher);
		}

	public:

		class Server {
			friend class Worker;

			public:
				typedef std::auto_ptr<Server> ptr;
				typedef void (*request_handler_t)(Worker& worker, const request_handle_t& handle, const std::vector<value_t>& args);
				typedef void (*message_handler_t)(Worker& worker, const std::vector<value_t>& args);

			private:
				int fd;
				struct ev_io accept_watcher;

				std::map<const std::string, request_handler_t> request_handlers;
				std::map<const std::string, message_handler_t> message_handlers;

				static void accept_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);

				/**
				 * Takes an existing listening fd and accepts new connections. Each new connection is allocated its
				 * own Worker instance with event handlers inherited from the server.
				 */
				Server(int fd) : fd(fd) {
					ev_io_init(&accept_watcher, accept_cb, fd, EV_READ);
					accept_watcher.data = this;
					ev_io_start(Worker::my_loop, &accept_watcher);
				};

			public:
				void register_handler(const std::string& request, const request_handler_t& handler) {
					request_handlers[request] = handler;
				}

				void register_handler(const std::string& message, const message_handler_t& handler) {
					message_handlers[message] = handler;
				}
		};

		/**
		 * Factory which generates a worker listening on a unix file socket.
		 */
		static Server::ptr listen(const std::string& path);

		/**
		 * Respond to a request from request_handler via the request_handle.
		 */
		void respond(const request_handle_t& handle, const value_t& value, bool threw = false);

		/**
		 * Run the ev_loop
		 */
		static void loop() {
			ev_loop(Worker::my_loop, 0);
		}

	private:
};

}
