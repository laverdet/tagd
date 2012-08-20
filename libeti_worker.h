#include <string>
#include <string.h>
#include <boost/threadpool.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/ptr_container/ptr_deque.hpp>
#include <ev.h>
#include <json_spirit.h>

namespace eti {

class Worker {
	public:
		class Server;
		typedef json_spirit::mValue value_t;
		typedef std::string request_handle_t;

	private:
		struct buffer_t {
			char* data;
			size_t offset;
			size_t length;
			buffer_t(const char* my_data, size_t length) : offset(0), length(length) {
				data = new char[length];
				memcpy(data, my_data, length);
			};
			~buffer_t() {
				delete[] data;
			}
		};

		static const size_t read_size = 4096;
		std::string read_buffer;
		boost::ptr_deque<buffer_t> write_buffer;
		boost::mutex write_lock;

		static struct ev_loop* my_loop;
		int fd;
		Server& server;
		struct ev_io fd_watcher;
		size_t outstanding_reqs;
		bool closed;

		static void fd_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);
		void read_cb();
		void write_cb();
		void handle_payload(const value_t& value);

		/**
		 * Private constructer called by Server.
		 */
		Worker(Server& server, int fd) : server(server), fd(fd), outstanding_reqs(0), closed(false) {
			ev_io_init(&fd_watcher, fd_cb, fd, EV_READ);
			fd_watcher.data = this;
			ev_io_start(Worker::my_loop, &fd_watcher);
		}

		void become_zombie() {
			boost::unique_lock<boost::mutex> lock(write_lock);
			closed = true;
			ev_io_stop(my_loop, &fd_watcher);
			close(fd);
			if (!outstanding_reqs) {
				lock.unlock();
				delete this;
			}
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
				boost::threadpool::pool threads;

				std::map<const std::string, request_handler_t> request_handlers;
				std::map<const std::string, message_handler_t> message_handlers;

				static void accept_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);
				static void request_wrapper(request_handler_t fn, Worker* worker, const request_handle_t handle, const std::vector<value_t> args);
				static void message_wrapper(message_handler_t fn, Worker* worker, const std::vector<value_t> args);

				/**
				 * Takes an existing listening fd and accepts new connections. Each new connection is allocated its
				 * own Worker instance with event handlers inherited from the server.
				 */
				Server(int fd) : fd(fd), threads(10) {
					// Ignore SIGPIPE. The internet says it's safe/recommended to do this.
					struct sigaction sa;
					sa.sa_handler = SIG_IGN;
					sa.sa_flags = 0;
					assert(sigemptyset(&sa.sa_mask) != -1);
					assert(sigaction(SIGPIPE, &sa, 0) != -1);

					// Start libev on this fd
					ev_io_init(&accept_watcher, accept_cb, fd, EV_READ);
					accept_watcher.data = this;
					ev_io_start(Worker::my_loop, &accept_watcher);
				}

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
