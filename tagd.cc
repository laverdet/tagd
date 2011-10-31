#include "libeti_worker.h"
#include <stdint.h>
#include <math.h>
#include <set>
#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#define foreach BOOST_FOREACH

using namespace std;
using namespace boost;
using namespace eti;

typedef uint32_t tag_t;
typedef uint64_t topic_id_t;
typedef uint32_t ts_t;
typedef set<tag_t> tag_set_t;

struct topic_t {
	topic_id_t id;
	ts_t ts;
	tag_set_t tags;
	topic_t(topic_id_t id, ts_t ts) : id(id), ts(ts) {};

	struct less {
		bool operator() (const topic_t* left, const topic_t* right) const {
			return left->ts > right->ts || (left->ts == right->ts && left->id > right->id);
		}
	};
};

typedef map<topic_id_t, topic_t*> topic_map_t;
typedef set<topic_t*, topic_t::less> topic_set_t;
typedef map<tag_t, topic_set_t*> tag_map_t;

/**
 * Abstract iterator for tagd expressions because I'm not smart enough to extend std::iterator.
 */
struct topic_iterator_t {
	typedef auto_ptr<topic_iterator_t> ptr;
	typedef ptr_vector<topic_iterator_t> ptr_vector_t;
	virtual ~topic_iterator_t() {};
	virtual void ff(const topic_t* ref) = 0;
	virtual topic_iterator_t& operator++ () = 0;
	virtual const topic_t* operator* () const = 0;
	const topic_t* operator-> () const {
		return **this;
	}
};

/**
 * Nothingness
 */
struct null_topic_iterator_t: public topic_iterator_t {
	virtual void ff(const topic_t* ref) {
		assert(false);
	}

	virtual null_topic_iterator_t& operator++ () {
		assert(false);
		return *this;
	}

	virtual topic_t* operator* () const {
		return NULL;
	}
};

/**
 * Basic adapter on top of topic_set_t.
 */
struct basic_topic_iterator_t: public topic_iterator_t {
	const topic_set_t& topic_set;
	topic_set_t::const_iterator it;

	basic_topic_iterator_t(const topic_set_t& topic_set) : topic_set(topic_set), it(topic_set.begin()) {};

	virtual void ff(const topic_t* ref) {
		assert(it != topic_set.end());
		assert(topic_t::less()(*it, ref) || !topic_t::less()(ref, *it));
		it = topic_set.lower_bound(const_cast<topic_t*>(ref));
	}

	virtual basic_topic_iterator_t& operator++ () {
		++it;
		return *this;
	}

	virtual const topic_t* operator* () const {
		return it == topic_set.end() ? NULL : *it;
	}
};

/**
 * Union iterator, returns topics in ANY of the iterators
 */
struct union_topic_iterator_t: public topic_iterator_t {
	auto_ptr<topic_iterator_t::ptr_vector_t> iterators;
	const topic_t* current;

	union_topic_iterator_t(auto_ptr<topic_iterator_t::ptr_vector_t> iterators) : iterators(iterators) {
		update();
	}

	void update() {
		// Find first topic from iterators
		topic_iterator_t::ptr_vector_t& iterators = *this->iterators;
		current = *iterators[0];
		for (size_t ii = 1; ii < iterators.size(); ++ii) {
			if (current == NULL || (*iterators[ii] && topic_t::less()(*iterators[ii], current))) {
				current = *iterators[ii];
			}
		};
	}

	virtual void ff(const topic_t* ref) {
		// Fast-forward each iterator individually
		foreach (topic_iterator_t& ii, *iterators) {
			if (*ii != NULL && topic_t::less()(*ii, ref)) {
				ii.ff(ref);
			}
		}
		update();
	}

	virtual union_topic_iterator_t& operator++ () {
		// Advance iterators which are currently pointing at the current topic
		foreach (topic_iterator_t& ii, *iterators) {
			if (*ii == current) {
				++ii;
			}
		}
		update();
		return *this;
	}

	virtual const topic_t* operator* () const {
		return current;
	}
};

/**
 * Intersection iterator, returns topics in ALL of the iterators
 */
struct intersection_topic_iterator_t: public topic_iterator_t {
	auto_ptr<topic_iterator_t::ptr_vector_t> iterators;
	const topic_t* current;

	intersection_topic_iterator_t(auto_ptr<topic_iterator_t::ptr_vector_t> iterators) : iterators(iterators) {
		update();
	}

	void update() {
		topic_iterator_t::ptr_vector_t& iterators = *this->iterators;
		const topic_t* oldest = *iterators[0];
		size_t oldest_ii = 0, ii = 1;

		do {
			// Keep cycling through all the iterators
			if (ii >= iterators.size()) {
				ii = 0;
			}
			if (*iterators[ii] == NULL) {
				// If we hit the end of any iterator there's no more intersections
				current = NULL;
				break;
			} else if (oldest_ii == ii) {
				// If we've made a cycle without fast forwarding then there's an intersection!
				current = oldest;
				break;
			}

			if (topic_t::less()(oldest, *iterators[ii])) {
				// Found a topic older
				oldest = *iterators[ii];
				oldest_ii = ii;
				++ii;
			} else if (topic_t::less()(*iterators[ii], oldest)) {
				// Not the same topic, fast forward and try again
				iterators[ii].ff(oldest);
			} else {
				// Topic found in more than one iterators
				++ii;
			}
		} while (true);
	}

	virtual void ff(const topic_t* ref) {
		// Fast-forward each iterator individually
		foreach (topic_iterator_t& ii, *iterators) {
			if (*ii != NULL && topic_t::less()(*ii, ref)) {
				ii.ff(ref);
			}
		}
		update();
	}

	virtual intersection_topic_iterator_t& operator++ () {
		// Advance all iterators
		foreach (topic_iterator_t& ii, *iterators) {
			++ii;
		}
		update();
		return *this;
	}

	virtual const topic_t* operator* () const {
		return current;
	}
};

/**
 * Difference iterator, returns topics that appear in `left`, but not `right`
 */
struct difference_topic_iterator_t: public topic_iterator_t {
	auto_ptr<topic_iterator_t> left;
	auto_ptr<topic_iterator_t> right;
	const topic_t* current;

	difference_topic_iterator_t(auto_ptr<topic_iterator_t> left, auto_ptr<topic_iterator_t> right) : left(left), right(right) {
		update();
	}

	void update() {
		do {
			if (!**left) {
				// If there's no more items in `left` it's done
				current = NULL;
				return;
			} else if (**right == NULL || topic_t::less()(**left , **right)) {
				// If `right` is older than `left` then `left` does not appear in `right`
				current = **left;
				return;
			} else if (topic_t::less()(**right, **left)) {
				// If `left` is older than `right` then it's inconclusive.. fast forward
				right->ff(**left);
			} else {
				// If they equal then an intersection has occured and this item in `left` should be skipped
				++*left;
				++*right;
			}
		} while (true);
	}

	virtual void ff(const topic_t* ref) {
		// Fast-forward both iterators individually
		left->ff(ref);
		if (**right != NULL && topic_t::less()(**right, ref)) {
			right->ff(ref);
		}
		update();
	}

	virtual difference_topic_iterator_t& operator++ () {
		// Advance both iterators
		++*left;
		if (**right != NULL) {
			++*right;
		}
		update();
		return *this;
	}

	virtual const topic_t* operator* () const {
		return current;
	}
};

tag_map_t topics_by_tags;
topic_map_t topics_by_id;

/**
 * Modifies the timestamp of a given topic.
 */
void exec_bump_topic(topic_id_t id, ts_t ts) {
	// Find the topic to enumerate tags
	topic_map_t::iterator ii = topics_by_id.find(id);
	if (ii == topics_by_id.end()) {
		return;
	}
	topic_t& topic = *ii->second;

	// Remove topic from each tag set before adjusting equality
	foreach (tag_t tag, topic.tags) {
		topics_by_tags.find(tag)->second->erase(&topic);
	}

	// Bump the topic and add back to tag sets
	topic.ts = ts;
	foreach (tag_t tag, topic.tags) {
		topics_by_tags.find(tag)->second->insert(&topic);
	}
}

/**
 * Message from the binlog watcher to update a topic's timestamp.
 */
void msg_bump_topic(Worker& worker, const vector<Worker::value_t>& args) {
	exec_bump_topic(args[0].get_uint64(), args[1].get_int());
}

/**
 * Message from the binlog watcher to associate a list of tags with a topic.
 */
void msg_add_tags(Worker& worker, const vector<Worker::value_t>& args) {
	topic_id_t id = args[0].get_uint64();
	ts_t ts = args[1].get_int();

	// Does the topic already exist?
	bool is_new;
  topic_map_t::iterator ii = topics_by_id.find(id);
	topic_t* topic;
  if (ii == topics_by_id.end()) {
		is_new = true;
		topic = new topic_t(id, ts);
		topics_by_id.insert(pair<topic_id_t, topic_t*>(id, topic));
	} else {
		is_new = false;
		topic = ii->second;
		if (topic->ts > ts) {
			// Old timestsamp?
			ts = topic->ts;
		} else if (topic->ts < ts) {
			// Need to bump?
			exec_bump_topic(id, ts);
		}
	}

	// Insert new tags
	const vector<Worker::value_t>& new_tags = args[2].get_array();
	foreach (const Worker::value_t& new_tag_val, new_tags) {
		tag_t new_tag = new_tag_val.get_int();
		// Check if this is already tagged
		if (is_new || topic->tags.find(new_tag) == topic->tags.end()) {
			// Need to add the topic to the tag bucket
			tag_map_t::iterator topic_set_it = topics_by_tags.find(new_tag);
			topic_set_t* topic_set;
			if (topic_set_it == topics_by_tags.end()) {
				// Never seen this tag before
				topic_set = new topic_set_t;
				topics_by_tags.insert(pair<tag_t, topic_set_t*>(new_tag, topic_set));
			} else {
				topic_set = topic_set_it->second;
			}
			topic_set->insert(topic);
			topic->tags.insert(new_tag);
		}
	}
}

/**
 * Builds an iterator from a JSON expression.
 */
topic_iterator_t::ptr build_iterator(const Worker::value_t& expr) {
	if (expr.type() == json_spirit::int_type) {
		tag_map_t::iterator it = topics_by_tags.find(expr.get_int());
		return it == topics_by_tags.end() ?
			topic_iterator_t::ptr(new null_topic_iterator_t()) :
			topic_iterator_t::ptr(new basic_topic_iterator_t(*it->second));
	} else if (expr.type() == json_spirit::array_type) {
		const vector<Worker::value_t>& exprs = expr.get_array();
		string type = exprs[0].get_str();
		if (type == "difference") {
			if (exprs.size() != 3) {
				throw runtime_error("unknown expression");
			}
			return topic_iterator_t::ptr(new difference_topic_iterator_t(build_iterator(exprs[1]), build_iterator(exprs[2])));
		} else {
			if (exprs.size() == 2) {
				return build_iterator(exprs[1]);
			} else if (exprs.size() == 1) {
				throw runtime_error("unknown expression");
			}
			auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t(exprs.size() - 1));
			for (size_t ii = 1; ii < exprs.size(); ++ii) {
				iterators->push_back(build_iterator(exprs[ii]));
			}
			if (type == "union") {
				return topic_iterator_t::ptr(new union_topic_iterator_t(iterators));
			} else if (type == "intersection") {
				return topic_iterator_t::ptr(new intersection_topic_iterator_t(iterators));
			} else {
				throw runtime_error("unknown expression");
			}
		}
	} else {
		throw runtime_error("unknown expression");
	}
}

/**
 * Request from a server for a slice of topics by expression
 */
void req_slice(Worker& worker, const Worker::request_handle_t& handle, const vector<Worker::value_t>& args) {
	// Initialize
	topic_iterator_t::ptr it = build_iterator(args[0]);
	size_t count = args[1].get_int();
	ts_t ff = args.size() > 2 ? (args[2].type() == json_spirit::bool_type ? args[2].get_bool() : args[2].get_int()) : 0;
	bool estimate_count = args.size() > 3 ? args[3].get_bool() : false;

	// Fastforward?
	if (ff) {
		auto_ptr<topic_t> fake_topic(new topic_t(0, ff));
		it->ff(&*fake_topic);
	}

	// Build results by id
	vector<Worker::value_t> results;
	ts_t first_ts = 0;
	for (const topic_t* ii = **it; ii && count; ii = *(++*it)) {
		if (first_ts == 0) {
			first_ts = ii->ts;
		}
		results.push_back(ii->id);
		--count;
	}

	map<string, Worker::value_t> response;
	response.insert(pair<string, Worker::value_t>("results", results));

	// Estimate count
	if (estimate_count) {
		if (count) {
			// Did we end up getting less than requested? No estimate required since the end was hit.
			response.insert(pair<string, Worker::value_t>("count", results.size()));
		} else {
			// Skip in exponentially wider chunks to guess the order of magnitude of results
			auto_ptr<topic_t> fake_topic(new topic_t(0, 0));
			size_t magnitude = round(log10(results.size())) - 2;
			while (**it) {
				fake_topic->ts = first_ts - (first_ts - (*it)->ts) * 10;
				if (fake_topic->ts > first_ts) {
					// Overflow?
					++magnitude;
					break;
				} else if (fake_topic->ts == first_ts) {
					// Same time posts
					++fake_topic->ts;
				}
				it->ff(&*fake_topic);
				++magnitude;
			}
			response.insert(pair<string, Worker::value_t>("count", pow(10, magnitude)));
		}
	}
	worker.respond(handle, response);
}

int main(const int argc, const char* argv[]) {
	if (argc != 2) {
		cout <<"usage: "<< argv[0] <<" <socket>\n";
		return 1;
	}
	Worker::Server::ptr server = Worker::listen(argv[1]);
	server->register_handler("addTags", msg_add_tags);
	server->register_handler("bumpTopic", msg_bump_topic);
	server->register_handler("slice", req_slice);
	Worker::loop();
	return 0;
}
