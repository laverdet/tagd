## tagd

This is a service used to organize and query discussions for an online
community. The server interface is a simple new-line delimited JSON stream
running over a UNIX socket. It runs in a single process with a thread for each
request.

After a client connects to the socket you can issue two types of payloads:
either a "message" or a "request". A message will yield no response, and a
request will emit a response-- again as a JSON payload written back to the
client.

To send a message, the client will write a JSON payload like this:
```
{"type":"message","name":"callback","payload":["json goes here"]}
```

This will pull a thread from a pool and call a callback in some C++ library
registered via Worker::Server::register_handle(...). No response will be
returned to the UNIX socket.

To issue a request, the client will write a JSON payload like this:
```
{"type":"request","name":"callback","uniq":1,"payload":<any JSON here>}
```

The main difference here is the addition of a `uniq` key. This should be a
unique key which will match a response to your request. The client should ensure
that this key is unique for all outgoing requests, otherwise there will be no
way to differentiate the incoming responses. Though, if multiple concurrent
clients are connecting they don't need to worry about coordinating unique keys
between them. After issuing a request, some time later the server will write
back to the socket a JSON payload like this:
```
{"type":"response","uniq":1,"data":<response goes here>}
```

Alternatively if the C++ callback throws a std::runtime_error then the error
string from that exception will be returned via:
```
{"type":"threw","uniq":1,"data":"err.what() goes here"}
```

Included are two services:

The first is a simple echo server which handles a single "echo" request. The
source code for that is included in `echod.cc` and you can build it via
`make echod`.

The second is the real hero of this package which is an in-memory index of
"topic" objects, each with any number of "tags" represented as a 32-bit integer.
After adding a topic to the index you can request a slice of topics via any
combination of tags, ordered by last post time. You can also request complex
expressions of tags such as "all topics tagged with tag 1, that don't include
any of tags 2 or 3". Unlimited nesting is supported here so the possibilities
are endless.

The service also runs a very basic, but fully live full-text search engine. The
full-text search is divided in two namespaces: "title" and "document". When
adding a topic to the index you may additionally specify a tokenized stream of
words which will be indexed. You can then query for those words using the same
expressions used for querying tags.

To get started check out `int main` in `tagd.cc` for a list of messages and
requests that the server accepts. To build run `make tagd`. You will need both
boost and json_spirit installed, as well as a sane C++ environment. It should
run fine on any any C++03 or later compiler, though only G++ has been tested.

Documentation here is sparse as this isn't really meant to be a widely-used
service. Instead it is offered as a case study for those interested in this kind
of thing. Currently I'm running this with an index size of ~80G with hundreds of
thousands of requests per day. Stability is very good with uptime reaching past
a year.
