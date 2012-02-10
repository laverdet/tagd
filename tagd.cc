#include "libeti_worker.h"
#include <stdint.h>
#include <math.h>
#include <set>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH

using namespace std;
using namespace boost;
using namespace eti;

boost::shared_mutex write_lock;

struct base_topic_t {
	typedef uint64_t id_t;
	typedef uint32_t ts_t;

	id_t id;
	ts_t ts;

	base_topic_t(id_t id, ts_t ts) : id(id), ts(ts) {};

	struct less {
		bool operator() (const base_topic_t* left, const base_topic_t* right) const {
			return left->ts > right->ts || (left->ts == right->ts && left->id > right->id);
		}
	};
};

struct topic_t: public base_topic_t {
	typedef multimap<struct word_t*, uint32_t> document_t;

	static map<id_t, base_topic_t*> topics_by_id;

	set<struct tag_t*> tags;
	set<struct word_t*> words;
	// leave this stuff in even though it's not being used, to get an idea of memory usage
	document_t document;
	vector<struct word_t*> vdocument;

	topic_t(id_t id, ts_t ts) : base_topic_t(id, ts) {};

	static topic_t* find(id_t id);
	static topic_t* find(id_t id, ts_t ts);
	static topic_t& get(id_t id, ts_t ts);
	void bump(ts_t ts);
};
map<topic_t::id_t, base_topic_t*> topic_t::topics_by_id;

struct tag_t {
	typedef uint32_t id_t;
	typedef set<topic_t*, topic_t::less> topic_set_t;
	static vector<tag_t*> tags_by_id;

	topic_set_t topics;

	static tag_t& get(id_t id);
};
vector<tag_t*> tag_t::tags_by_id(1, NULL);

struct word_t {
	typedef set<topic_t*, topic_t::less> topic_set_t;
	static map<const string, word_t*> words_by_string;

	const string word;
	topic_set_t topics;

	word_t(const string& word) : word(word) {}

	static word_t* find(const string& id);
	static word_t& get(const string& id);
};
map<const string, word_t*> word_t::words_by_string;

topic_t* topic_t::find(id_t id) {
	map<id_t, base_topic_t*>::iterator ii = topics_by_id.find(id);
	if (ii == topics_by_id.end()) {
		return NULL;
	}
	return static_cast<topic_t*>(ii->second);
}

topic_t* topic_t::find(id_t id, ts_t ts) {
	map<id_t, base_topic_t*>::iterator ii = topics_by_id.find(id);
	if (ii == topics_by_id.end()) {
		return NULL;
	}

	topic_t& topic = *static_cast<topic_t*>(ii->second);
	topic.bump(ts);
	return &topic;
}

topic_t& topic_t::get(id_t id, ts_t ts) {
	topic_t* topic = find(id, ts);
	if (topic) {
		return *topic;
	}

	topic = new topic_t(id, ts);
	topics_by_id.insert(make_pair(id, topic));
	return *topic;
}

void topic_t::bump(ts_t ts) {
	if (this->ts >= ts) {
		return;
	}

	// Remove topic from each tag set before adjusting equality
	foreach (tag_t* tag, tags) {
		tag->topics.erase(this);
	}
	foreach (word_t* word, words) {
		word->topics.erase(this);
	}

	// Bump the topic and add back to tag sets
	this->ts = ts;
	foreach (tag_t* tag, tags) {
		tag->topics.insert(this);
	}
	foreach (word_t* word, words) {
		word->topics.insert(this);
	}
}

tag_t& tag_t::get(id_t id) {
	if (tags_by_id.size() < id) {
		tags_by_id.resize(id, NULL);
	}
	tag_t* tag = tags_by_id[id - 1];
	if (tag == NULL) {
		tag = new tag_t();
		tags_by_id[id - 1] = tag;
	}
	return *tag;
}

word_t* word_t::find(const string& str) {
	map<const string, word_t*>::iterator ii = words_by_string.find(str);
	if (ii != words_by_string.end()) {
		return ii->second;
	}
	return NULL;
}

word_t& word_t::get(const string& str) {
	map<const string, word_t*>::iterator ii = words_by_string.find(str);
	if (ii != words_by_string.end()) {
		return *ii->second;
	}
	word_t& word = *new word_t(str);
	words_by_string.insert(make_pair(str, &word));
	return word;
}

/**
 * Abstract iterator for tagd expressions because I'm not smart enough to extend std::iterator.
 */
struct topic_iterator_t {
	typedef auto_ptr<topic_iterator_t> ptr;
	typedef ptr_vector<topic_iterator_t> ptr_vector_t;
	virtual ~topic_iterator_t() {};
	virtual void ff(const base_topic_t* ref) = 0;
	virtual size_t max() const = 0;
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
	virtual void ff(const base_topic_t* ref) {
		assert(false);
	}

	virtual size_t max() const {
		return 0;
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
	const tag_t::topic_set_t& topic_set;
	tag_t::topic_set_t::const_iterator it;

	basic_topic_iterator_t(const tag_t::topic_set_t& topic_set) : topic_set(topic_set), it(topic_set.begin()) {};

	virtual void ff(const base_topic_t* ref) {
		assert(it != topic_set.end());
		assert(topic_t::less()(*it, ref) || !topic_t::less()(ref, *it));
		it = topic_set.lower_bound(static_cast<topic_t*>(const_cast<base_topic_t*>(ref)));
	}

	virtual size_t max() const {
		return topic_set.size();
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

	virtual void ff(const base_topic_t* ref) {
		// Fast-forward each iterator individually
		foreach (topic_iterator_t& ii, *iterators) {
			if (*ii != NULL && topic_t::less()(*ii, ref)) {
				ii.ff(ref);
			}
		}
		update();
	}

	virtual size_t max() const {
		size_t max = 0;
		foreach (topic_iterator_t& iterator, *iterators) {
			size_t my_max = iterator.max();
			if (my_max > max) {
				max = my_max;
			}
		}
		return max;
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

		if (oldest == NULL) {
			current = NULL;
			return;
		}
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

	virtual void ff(const base_topic_t* ref) {
		// Fast-forward each iterator individually
		foreach (topic_iterator_t& ii, *iterators) {
			if (*ii != NULL && topic_t::less()(*ii, ref)) {
				ii.ff(ref);
			}
		}
		update();
	}

	virtual size_t max() const {
		size_t min = 0xffffffff;
		foreach (topic_iterator_t& iterator, *iterators) {
			size_t my_max = iterator.max();
			if (my_max < min) {
				min = my_max;
			}
		}
		return min;
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

	virtual void ff(const base_topic_t* ref) {
		// Fast-forward both iterators individually
		left->ff(ref);
		if (**right != NULL && topic_t::less()(**right, ref)) {
			right->ff(ref);
		}
		update();
	}

	virtual size_t max() const {
		return left->max();
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

/**
 * Full-text phrase iterator. Returns topics matching a full-text iterator.
 *
 * INCOMPLETE: I stopped coding this at some point. Pick this back up when you feel like it.
 */
topic_iterator_t::ptr build_wildcard_iterator(const string& word);
struct phrase_topic_iterator_t: public topic_iterator_t {
	typedef vector<string> word_vector_t;

	auto_ptr<topic_iterator_t> base_iterator;
	auto_ptr<word_vector_t> tokens;
	vector<word_t*> words;
	const topic_t* current;

	phrase_topic_iterator_t(auto_ptr<word_vector_t> tokens) : tokens(tokens), words(this->tokens->size(), NULL) {
		auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t);
		size_t ii = 0;
		foreach (const string& token, *this->tokens) {
			++ii;
			if (token[token.length() - 1] == '*') {
				if (ii == 1) {
					throw runtime_error("invalid phrase search");
				}
				string no_star(token.substr(0, token.length() - 1));
				try {
					iterators->push_back(build_wildcard_iterator(no_star));
				} catch (const runtime_error& error) {
					// don't worry about too many matches
				}
			} else if (token == "*") {
				if (ii == 1) {
					throw runtime_error("invalid phrase search");
				}
				continue;
			} else {
				word_t* word = word_t::find(token);
				words[ii - 1] = word;
				if (word) {
					iterators->push_back(topic_iterator_t::ptr(new basic_topic_iterator_t(word->topics)));
				} else {
					iterators->clear();
					iterators->push_back(topic_iterator_t::ptr(new null_topic_iterator_t));
					break;
				}
			}
		}

		if (iterators->size() == 0) {
			throw runtime_error("invalid phrase search");
		} else if (iterators->size() == 1) {
			base_iterator = topic_iterator_t::ptr(iterators->pop_back().release());
		} else {
			base_iterator = topic_iterator_t::ptr(new intersection_topic_iterator_t(iterators));
		}

		update();
	}

	bool matches(const topic_t*, size_t word, uint32_t cursor) {
		if (word == words.size()) {
			return true;
		}
	}

	void update() {
		const topic_t* topic = **base_iterator;
		for (const topic_t* topic = **base_iterator; topic; ++*base_iterator) {
			pair<topic_t::document_t::const_iterator, topic_t::document_t::const_iterator> seed_range(topic->document.equal_range(words[0]));
			for (; seed_range.first != seed_range.second; ++seed_range.first) {
				if (matches(topic, 1, seed_range.first->second + 1)) {
					current = topic;
					break;
				}
			}
		}
		current = NULL;
		return;
	}

	virtual void ff(const base_topic_t* ref) {
		base_iterator->ff(ref);
		update();
	}

	virtual size_t max() const {
		return base_iterator->max();
	}

	virtual phrase_topic_iterator_t& operator++ () {
		++*base_iterator;
		update();
		return *this;
	}

	virtual const topic_t* operator* () const {
		return current;
	}
};


/**
 * Message from the binlog watcher to update a topic's timestamp.
 */
void msg_bump_topic(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::id_t id = args[0].get_uint64();
	topic_t::ts_t ts = args[1].get_int();

	topic_t* topic = topic_t::find(id);
	if (topic) {
		topic->bump(ts);
	}
}

/**
 * Message from the binlog watcher to associate a list of tags with a topic.
 */
void msg_add_tags(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::id_t id = args[0].get_uint64();
	topic_t::ts_t ts = args[1].get_int();
	const vector<Worker::value_t>& new_tags = args[2].get_array();

	topic_t& topic = topic_t::get(id, ts);

	foreach (const Worker::value_t& new_tag_val, new_tags) {
		tag_t& tag = tag_t::get(new_tag_val.get_int());
		tag.topics.insert(&topic);
		topic.tags.insert(&tag);
	}
}

/**
 * Message from the binlog watcher to remove a tag.
 */
void msg_remove_tag(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::id_t id = args[0].get_uint64();
	tag_t::id_t tag_id = args[1].get_uint64();

	// Find the topic
	topic_t* topic = topic_t::find(id);
	if (!topic) {
		return;
	}

	// Remove the tag
	tag_t& tag = tag_t::get(tag_id);
	tag.topics.erase(topic);
	topic->tags.erase(&tag);
}

/**
 * Sets the full-text search content of a topic.
 */
void msg_full_text(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::id_t id = args[0].get_uint64();
	topic_t::ts_t ts = args[1].get_int();
	const vector<Worker::value_t>& document = args[2].get_array();

	topic_t& topic = topic_t::get(id, ts);
	set<word_t*> original_words;
	for (multimap<word_t*, uint32_t>::iterator word = topic.document.begin(); word != topic.document.end(); ++word) {
		original_words.insert(word->first);
	}
	topic.document.clear();
	topic.vdocument.resize(document.size());
	topic.words.clear();
	uint32_t ii = 0;
	foreach (const Worker::value_t& token, document) {
		word_t& word = word_t::get(token.get_str());
		topic.document.insert(make_pair(&word, ii));
		topic.vdocument[ii] = &word;
		topic.words.insert(&word);
		++ii;
	}

	set<word_t*>::iterator left = original_words.begin();
	set<word_t*>::iterator right = topic.words.begin();
	while (left != original_words.end() && right != topic.words.end()) {
		if (*left == *right) {
			++left;
			++right;
		} else if (*left < *right) {
			(*left)->topics.erase(&topic);
			++left;
		} else {
			(*right)->topics.insert(&topic);
			++right;
		}
	}
	while (left != original_words.end()) {
		(*left)->topics.erase(&topic);
		++left;
	}
	while (right != topic.words.end()) {
		(*right)->topics.insert(&topic);
		++right;
	}
}

/**
 * Builds an iterator from a wildcard word match. Throws if this wildcard is unreasonable.
 */
topic_iterator_t::ptr build_wildcard_iterator(const string& word) {
	size_t total_matches = 0;
	auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t);
	map<const string, word_t*>::iterator it = word_t::words_by_string.lower_bound(word);
	while (it != word_t::words_by_string.end() && it->first.compare(0, word.length(), word) == 0) {
		topic_iterator_t::ptr new_iterator(new basic_topic_iterator_t(it->second->topics));
		total_matches += new_iterator->max();
		iterators->push_back(new_iterator);
		if (total_matches > topic_t::topics_by_id.size() / 4) {
			throw runtime_error("too many matches");
		}
		++it;
	}
	if (iterators->size() == 0) {
		return topic_iterator_t::ptr(new null_topic_iterator_t);
	}
	return topic_iterator_t::ptr(new union_topic_iterator_t(iterators));
}

/**
 * Builds an iterator from a JSON expression.
 */
topic_iterator_t::ptr build_iterator(const Worker::value_t& expr) {
	if (expr.type() == json_spirit::int_type) {
		tag_t& tag = tag_t::get(expr.get_int());
		return topic_iterator_t::ptr(new basic_topic_iterator_t(tag.topics));
	} else if (expr.type() == json_spirit::str_type) {
		const string& word = expr.get_str();
		if (word.length() >= 2 && word[word.length() - 1] == '*') {
			// prefix search
			string word = expr.get_str();
			word.erase(word.length() - 1, 1);
			return build_wildcard_iterator(word);
		} else {
			word_t* word = word_t::find(expr.get_str());
			if (word) {
				return topic_iterator_t::ptr(new basic_topic_iterator_t(word->topics));
			}
			return topic_iterator_t::ptr(new null_topic_iterator_t);
		}
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

	try {
		// Initialize
		boost::shared_lock<boost::shared_mutex> lock(write_lock);
		topic_iterator_t::ptr it = build_iterator(args[0]);
		size_t count = args[1].get_int();
		topic_t::ts_t ff = args.size() > 2 ? (args[2].type() == json_spirit::int_type ? args[2].get_int() : 0) : 0;
		bool estimate_count = args.size() > 3 ? (args[3].type() == json_spirit::bool_type ? args[3].get_bool() : false) : false;

		// Fastforward?
		if (ff) {
			auto_ptr<base_topic_t> fake_topic(new base_topic_t(0, ff));
			it->ff(&*fake_topic);
		}

		// Build results by id
		vector<Worker::value_t> results;
		topic_t::ts_t first_ts = 0;
		for (const topic_t* ii = **it; ii && count; ii = *(++*it)) {
			if (first_ts == 0) {
				first_ts = ii->ts;
			}
			results.push_back(ii->id);
			--count;
		}

		map<string, Worker::value_t> response;
		response.insert(make_pair("results", results));

		// Estimate count
		if (estimate_count) {
			if (count) {
				// Did we end up getting less than requested? No estimate required since the end was hit.
				response.insert(make_pair("count", results.size()));
			} else {
				// Skip in exponentially wider chunks to guess the order of magnitude of results
				auto_ptr<base_topic_t> fake_topic(new base_topic_t(0, 0));
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
				response.insert(make_pair("count", pow(10, magnitude)));
			}
		}
		worker.respond(handle, response);
	} catch (const runtime_error& error) {
		worker.respond(handle, error.what(), true);
	}
}

int main(const int argc, const char* argv[]) {
	if (argc != 2) {
		cout <<"usage: " <<argv[0] <<" <socket>\n";
		return 1;
	}
	Worker::Server::ptr server = Worker::listen(argv[1]);
	server->register_handler("addTags", msg_add_tags);
	server->register_handler("removeTag", msg_remove_tag);
	server->register_handler("bumpTopic", msg_bump_topic);
	server->register_handler("fullText", msg_full_text);
	server->register_handler("slice", req_slice);
	Worker::loop();
	return 0;
}
