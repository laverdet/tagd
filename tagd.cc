#include "libeti_worker.h"
#include <stdint.h>
#include <math.h>
#include <sys/time.h>
#include <set>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH
#define reverse_foreach BOOST_REVERSE_FOREACH

const double message_cutoff = 43200;
const double topic_cutoff = 86400 * 5;
const size_t inverse_req = 10000;

using namespace std;
using namespace boost;
using namespace eti;

boost::shared_mutex write_lock;

struct base_topic_t {
	typedef uint64_t id_t;
	typedef uint32_t ts_t;
	typedef uint32_t user_t;

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
	typedef pair<ts_t, user_t> post_t;

	static map<id_t, base_topic_t*> topics_by_id;

	set<struct tag_t*> tags;
	set<struct word_t*> words;
	set<post_t> messages;
	map<user_t, uint32_t> message_counts;
	ts_t created;

	topic_t(id_t id, ts_t ts) : base_topic_t(id, ts), created(0) {};

	static topic_t* find(id_t id);
	static topic_t* find(id_t id, ts_t ts);
	static topic_t& get(id_t id, ts_t ts);
	void bump(ts_t ts);
	double score() const;
};
map<topic_t::id_t, base_topic_t*> topic_t::topics_by_id;

struct tag_t {
	typedef uint32_t id_t;
	typedef set<topic_t*, topic_t::less> topic_set_t;
	static vector<tag_t*> tags_by_id;
	static vector<tag_t*> inverse_tags;
	static tag_t active_tag;
	static tag_t global_tag;

	topic_set_t topics;
	tag_t* inverse_tag;

	tag_t() : inverse_tag(NULL) {};

	static tag_t& get(id_t id);
};
vector<tag_t*> tag_t::tags_by_id(1, NULL);
vector<tag_t*> tag_t::inverse_tags;
tag_t tag_t::active_tag;
tag_t tag_t::global_tag;

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
	tag_t::global_tag.topics.insert(topic);
	topic->tags.insert(&tag_t::global_tag);
	foreach (tag_t* tag, tag_t::inverse_tags) {
		tag->topics.insert(topic);
		topic->tags.insert(tag);
	}
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

double topic_t::score() const {
	return (1 - pow((time(NULL) - created) / topic_cutoff, 2)) * message_counts.size();
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
		// Only need to advance the left iterator since right will be advanced in update()
		++*left;
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
	topic_t::user_t user = args[2].get_int();

	topic_t* topic = topic_t::find(id);
	if (topic) {
		topic->bump(ts);
		if (time(NULL) - topic_cutoff < topic->created) {
			topic->messages.insert(make_pair(ts, user));
			++topic->message_counts[user];
			tag_t::active_tag.topics.insert(topic);
			topic->tags.insert(&tag_t::active_tag);
		}
	}
}

/**
 * Message from the binlog watcher when a topic is created.
 */
void msg_created_topic(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::id_t id = args[0].get_uint64();
	topic_t::ts_t ts = args[1].get_int();

	topic_t& topic = topic_t::get(id, ts);
	topic.created = ts;
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
		if (tag.inverse_tag) {
			// Remove from inverse tag
			tag_t& inverse = *tag.inverse_tag;
			if (topic.tags.erase(&inverse) == 0) {
				// This topic was not found in the inverse tag and therefore already exists in the normal
				// tag
				continue;
			}
			inverse.topics.erase(&topic);
		}
		// Add to tag
		tag.topics.insert(&topic);
		topic.tags.insert(&tag);

		// Is this tag big enough to need its inverse created?
		if (
			tag.topics.size() * 2 > tag_t::global_tag.topics.size() &&
			!tag.inverse_tag &&
			inverse_req < tag_t::global_tag.topics.size()
		) {
			tag_t& inverse = *new tag_t;
			tag.inverse_tag = &inverse;
			inverse.inverse_tag = &tag;
			tag_t::inverse_tags.push_back(&inverse);
			foreach (topic_t* topic, tag_t::global_tag.topics) {
				if (topic->tags.find(&tag) == topic->tags.end()) {
					topic->tags.insert(&inverse);
					inverse.topics.insert(topic);
				}
			}
		}
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
	if (topic->tags.erase(&tag) == 0) {
		// This topic was not tagged at all, nothing else to do in this function
		return;
	}
	tag.topics.erase(topic);

	// Add the inverse tag if needed
	if (tag.inverse_tag) {
		tag_t& inverse = *tag.inverse_tag;
		inverse.topics.insert(topic);
		topic->tags.insert(&inverse);
	}
}

/**
 * Message to remove this tag from *all* topics. This is used to start over from
 * scratch on a tag, after retraining autotag.
 */
void msg_clear_tag(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	tag_t::id_t tag_id = args[0].get_uint64();

	tag_t& tag = tag_t::get(tag_id);

	// First, do we need to add all these topics to the inverse?
	if (tag.inverse_tag) {
		tag_t& inverse = *tag.inverse_tag;
		foreach (topic_t* topic, tag.topics) {
			topic->tags.insert(&inverse);
			inverse.topics.insert(topic);
		}
	}

	// Remove tag from all topics
	foreach (topic_t* topic, tag.topics) {
		topic->tags.erase(&tag);
	}
	tag.topics.clear();
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
	set<word_t*> original_words(topic.words);
	topic.words.clear();
	uint32_t ii = 0;
	foreach (const Worker::value_t& token, document) {
		word_t& word = word_t::get(token.get_str());
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
 * Every 5 minutes or whatever this is called to go through and to pull out old messages from each
 * topic's `messages` and `message_counts` objects.
 */
void msg_flush_counts(Worker& worker, const vector<Worker::value_t>& args) {
	boost::lock_guard<boost::shared_mutex> lock(write_lock);
	topic_t::ts_t ts = time(NULL);

	// Loop through each topic with an active message
	tag_t::topic_set_t::iterator ii = tag_t::active_tag.topics.begin();
	while (ii != tag_t::active_tag.topics.end()) {
		topic_t& topic = **ii;

		// Loop through all messages in the topic
		set<topic_t::post_t>::iterator jj = topic.messages.begin();
		while (jj != topic.messages.end()) {

			// If this post is older than the cutoff remove it
			if (jj->first < ts - message_cutoff) {
				map<topic_t::user_t, uint32_t>::iterator count_iterator = topic.message_counts.find(jj->second);
				if (count_iterator->second == 1) {
					topic.message_counts.erase(count_iterator);
				} else {
					--count_iterator->second;
				}
				topic.messages.erase(jj++);
			} else {
				break;
			}
		}

		// No more active messages?
		if (topic.messages.empty()) {
			tag_t::active_tag.topics.erase(ii++);
			topic.tags.erase(&tag_t::active_tag);
		} else {
			++ii;
		}
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
		int val = expr.get_int();
		tag_t* tag;
		if (val) {
			tag = &tag_t::get(expr.get_int());
		} else {
			tag = &tag_t::global_tag;
		}
		return topic_iterator_t::ptr(new basic_topic_iterator_t(tag->topics));
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
			// Look for things to convert from [diff, a, b] to [intersect, a, ~b]
			if (exprs[2].type() == json_spirit::int_type && exprs[2].get_int()) {
				tag_t& tag = tag_t::get(exprs[2].get_int());
				if (tag.inverse_tag) {
					// Single difference expr with an inverse
					auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t(2));
					iterators->push_back(build_iterator(exprs[1]));
					iterators->push_back(new basic_topic_iterator_t(tag.inverse_tag->topics));
					return topic_iterator_t::ptr(new intersection_topic_iterator_t(iterators));
				}
			} else if (exprs[2].type() == json_spirit::array_type) {
				const vector<Worker::value_t>& exprs2 = exprs[2].get_array();
				if (exprs2[0].get_str() == "union") {
					auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t(0));
					auto_ptr<topic_iterator_t::ptr_vector_t> inverse_iterators(new topic_iterator_t::ptr_vector_t(0));
					for (size_t ii = 1; ii < exprs2.size(); ++ii) {
						if (exprs2[ii].type() == json_spirit::int_type && exprs2[ii].get_int()) {
							tag_t& tag = tag_t::get(exprs2[ii].get_int());
							if (tag.inverse_tag) {
								inverse_iterators->push_back(new basic_topic_iterator_t(tag.inverse_tag->topics));
								continue;
							}
						}
						iterators->push_back(build_iterator(exprs2[ii]));
					}
					topic_iterator_t::ptr iterator = build_iterator(exprs[1]);
					if (inverse_iterators->size()) {
						inverse_iterators->push_back(iterator);
						iterator = topic_iterator_t::ptr(new intersection_topic_iterator_t(inverse_iterators));
					}
					if (iterators->size()) {
						iterator = topic_iterator_t::ptr(new difference_topic_iterator_t(
							iterator,
							topic_iterator_t::ptr(new union_topic_iterator_t(iterators))
						));
					}
					return iterator;
				}
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
			const topic_t* first_topic = **it;
			if (first_topic != NULL && first_topic->ts > ff) {
				auto_ptr<base_topic_t> fake_topic(new base_topic_t(0, ff));
				it->ff(&*fake_topic);
			}
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
			if (count || **it == NULL) {
				// Did we end up getting less than requested? No estimate required since the end was hit.
				response.insert(make_pair("count", results.size()));
			} else {
				// Jump up to result #2500
				size_t skip_forward = results.size();
				while (skip_forward < 2500) {
					++skip_forward;
					++*it;
					if (**it == NULL) {
						response.insert(make_pair("count", skip_forward));
						goto skip_estimate; // woohoo!
					}
				}

				// Skip in exponentially wider chunks to guess the order of magnitude of results
				auto_ptr<base_topic_t> fake_topic(new base_topic_t(0, 0));
				double magnitude = log2(skip_forward);
				base_topic_t::ts_t last_ts = first_ts;
				while (**it) {
					fake_topic->ts = first_ts - (first_ts - (*it)->ts) * 2;
					if (fake_topic->ts > last_ts) {
						// Overflow?
						++magnitude;
						break;
					} else if (fake_topic->ts == last_ts) {
						// Same time posts
						--fake_topic->ts;
					}
					it->ff(&*fake_topic);
					last_ts = fake_topic->ts;
					++magnitude;
				}
				response.insert(make_pair("count", round(pow(2, magnitude))));
				response.insert(make_pair("estimated", true));
			}
skip_estimate:;
		}
		worker.respond(handle, response);
	} catch (const runtime_error& error) {
		worker.respond(handle, error.what(), true);
	}
}

/**
 * Request for most active topics from an expression
 */
typedef pair<double, const topic_t*> score_topic_pair_t;
void req_hot(Worker& worker, const Worker::request_handle_t& handle, const vector<Worker::value_t>& args) {

	// Initialize
	boost::shared_lock<boost::shared_mutex> lock(write_lock);
	auto_ptr<topic_iterator_t::ptr_vector_t> iterators(new topic_iterator_t::ptr_vector_t);
	iterators->push_back(topic_iterator_t::ptr(new basic_topic_iterator_t(tag_t::active_tag.topics)));
	iterators->push_back(build_iterator(args[0]));
	topic_iterator_t::ptr it(new intersection_topic_iterator_t(iterators));
	uint32_t count = args[1].get_int();

	// Push results into a set to sort
	set<pair<double, const topic_t*> > results;
	for (const topic_t* ii = **it; ii; ii = *(++*it)) {
		results.insert(make_pair(ii->score(), ii));
	}

	// Generate payload
	vector<Worker::value_t> json;
	reverse_foreach(const score_topic_pair_t& ii, results) {
		json.push_back(ii.second->id);
		if (!--count) {
			break;
		}
	}
	worker.respond(handle, json);
}

/**
 * Simply lock and unlock the service. This is useful to make sure writes have caught up.
 */
void req_sync(Worker& worker, const Worker::request_handle_t& handle, const vector<Worker::value_t>& args) {
	write_lock.lock();
	write_lock.unlock();
	worker.respond(handle, Worker::value_t(true));
}

int main(const int argc, const char* argv[]) {
	if (argc != 2) {
		cout <<"usage: " <<argv[0] <<" <socket>\n";
		return 1;
	}
	Worker::Server::ptr server = Worker::listen(argv[1]);
	server->register_handler("addTags", msg_add_tags);
	server->register_handler("removeTag", msg_remove_tag);
	server->register_handler("clearTag", msg_clear_tag);
	server->register_handler("bumpTopic", msg_bump_topic);
	server->register_handler("createTopic", msg_created_topic);
	server->register_handler("fullText", msg_full_text);
	server->register_handler("flushCounts", msg_flush_counts);
	server->register_handler("slice", req_slice);
	server->register_handler("hot", req_hot);
	server->register_handler("sync", req_sync);
	Worker::loop();
	return 0;
}
