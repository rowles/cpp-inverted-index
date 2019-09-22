#include <cassert>
#include <algorithm>
#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <string_view>
#include <optional>


using DocId = uint64_t;
using DocIdVec = std::vector<DocId>;


// serialize vector of fixed size primitives
namespace serialize {

template <class T>
std::ostream& write_vector(std::ostream &os, const std::vector<T> &data) {
  const auto len = data.size();

  os.write(reinterpret_cast<const char *>(&len), sizeof(len));
  os.write(reinterpret_cast<const char *>(&data[0]), len * sizeof(T));

  return os;
}

template <class T>
std::istream& read_vector(std::istream &is, std::vector<T> &data) {
  size_t len = 0;
  
  is.read(reinterpret_cast<char *>(&len), sizeof(size_t));
  data.resize(len);
  is.read(reinterpret_cast<char *>(&data[0]), sizeof(T) * len);

  return is;
}

} // namespace serialize


namespace iidx {

// Simple Key Value Store Interface
//
// drop in replacement for leveldb or other string kv
//
class KVStore {
public:
  KVStore() = default;
  virtual ~KVStore() = default;

  void insert(const std::string &k, const std::string &v) {
    this->_store[k] = v;
  }

  std::string get(const std::string &k) {
    return this->_store[k];
  }

  bool exists(const std::string &k) {
    return this->_store.find(k) != this->_store.end();
  }

private:
  std::unordered_map<std::string, std::string> _store;
};

// Inverted Index
//
// Maintains term to document id vector mapping
//
class IIndex {
public:
  IIndex() = default;
  virtual ~IIndex() = default;

  void add_term(const DocId did, const std::string& term) {
    if (!this->_kvstore.exists(term)) {
      this->add_new_term(did, term);
    } else {
      this->add_existing_term(did, term);
    }
  }

  std::optional<DocIdVec> get_doc_vector(const std::string& term) {
    if (this->_kvstore.exists(term)) {
      const auto vecstr = this->_kvstore.get(term);

      std::stringstream ss (vecstr);

      // read doc vector
      DocIdVec doc_vec;
      serialize::read_vector(ss, doc_vec);
      return doc_vec;
    }

    return {};
  }

private:
  void add_new_term(const DocId did, const std::string &term) {
    DocIdVec doc_vec = {did};

    std::stringstream ss{};

    serialize::write_vector(ss, doc_vec);

    this->_kvstore.insert(term, ss.str());
  }

  void add_existing_term(const DocId did, const std::string &term) {
    const auto vecstr = this->_kvstore.get(term);
    std::stringstream ss (vecstr);

    // read doc vector
    auto doc_vec = this->get_doc_vector(term).value();

    // add new doc id
    // do not allow duplicates
    if (!std::binary_search(doc_vec.begin(), doc_vec.end(), did)) {

      const auto it = std::lower_bound(
          doc_vec.begin(), doc_vec.end(), did, std::less<DocId>());

      // maintain sorted vector
      doc_vec.insert(it, did);

      std::stringstream().swap(ss);
      serialize::write_vector(ss, doc_vec);

      this->_kvstore.insert(term, ss.str());
    }
  }

  KVStore _kvstore{};
};
} // namespace iidx


// helper to print doc vec
DocIdVec print_doc_vector(iidx::IIndex &idx, const std::string &term) {
  const auto dvec = idx.get_doc_vector(term);
  std::cout << term << ": ";
  if (dvec) {
    for (auto i : dvec.value())
      std::cout << i << ' ';

    std::cout << '\n';
    return dvec.value();
  } else {
    std::cout << "not found\n";
    return {};
  }
}


int main() {
  iidx::IIndex idx{};

  idx.add_term(0, "dog");
  idx.add_term(0, "cat");
  idx.add_term(1, "cat");
  idx.add_term(1, "mouse");
  idx.add_term(1, "house");
  idx.add_term(2, "cat");
  idx.add_term(2, "dog");

  // doc ids should be sorted!
  idx.add_term(2, "tree");
  idx.add_term(1, "tree");


  auto v0 = print_doc_vector(idx, "cat");
  assert((v0 == DocIdVec{0, 1, 2}));

  auto v1 = print_doc_vector(idx, "mouse");
  assert((v1 == DocIdVec{1}));
  
  auto v2 = print_doc_vector(idx, "dog");
  assert((v2 == DocIdVec{0, 2}));
  
  auto v3 = print_doc_vector(idx, "house");
  assert((v3 == DocIdVec{1}));
  
  auto v4 = print_doc_vector(idx, "tree");
  assert((v4 == DocIdVec{1, 2}));
}


