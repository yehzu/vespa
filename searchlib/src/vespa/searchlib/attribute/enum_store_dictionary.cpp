// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "enum_store_dictionary.h"
#include "enumstore.h"
#include <vespa/vespalib/btree/btree.hpp>
#include <vespa/vespalib/btree/btreeiterator.hpp>
#include <vespa/vespalib/btree/btreenode.hpp>
#include <vespa/vespalib/btree/btreenodeallocator.hpp>
#include <vespa/vespalib/btree/btreeroot.hpp>
#include <vespa/vespalib/datastore/datastore.hpp>
#include <vespa/vespalib/datastore/unique_store_dictionary.hpp>
#include <vespa/vespalib/util/bufferwriter.h>

#include <vespa/log/log.h>
LOG_SETUP(".searchlib.attribute.enum_store_dictionary");

using search::datastore::EntryComparator;
using search::datastore::EntryRef;
using search::datastore::UniqueStoreAddResult;

namespace search {

using btree::BTreeNode;

template <typename DictionaryT>
EnumStoreDictionary<DictionaryT>::EnumStoreDictionary(IEnumStore& enumStore)
    : ParentUniqueStoreDictionary(),
      _enumStore(enumStore)
{
}

template <typename DictionaryT>
EnumStoreDictionary<DictionaryT>::~EnumStoreDictionary() = default;

template <typename DictionaryT>
uint32_t
EnumStoreDictionary<DictionaryT>::getNumUniques() const
{
    return this->_dict.size();
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::writeAllValues(BufferWriter& writer,
                                                 BTreeNode::Ref rootRef) const
{
    constexpr size_t BATCHSIZE = 1000;
    std::vector<Index> idxs;
    idxs.reserve(BATCHSIZE);
    typename DictionaryT::Iterator it(rootRef, this->_dict.getAllocator());
    while (it.valid()) {
        if (idxs.size() >= idxs.capacity()) {
            _enumStore.writeValues(writer, vespalib::ConstArrayRef(&idxs[0], idxs.size()));
            idxs.clear();
        }
        idxs.push_back(it.getKey());
        ++it;
    }
    if (!idxs.empty()) {
        _enumStore.writeValues(writer, vespalib::ConstArrayRef(&idxs[0], idxs.size()));
    }
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::fixupRefCounts(const EnumVector& hist)
{
    _enumStore.fixupRefCounts(hist, this->_dict);
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::removeUnusedEnums(const IndexSet& unused,
                                                    const datastore::EntryComparator& cmp)
{
    if (unused.empty()) {
        return;
    }
    for (const auto& ref : unused) {
        this->remove(cmp, ref);
    }
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::freeUnusedEnums(const datastore::EntryComparator& cmp)
{
    IndexSet unused;

    // find unused enums
    for (auto iter = this->_dict.begin(); iter.valid(); ++iter) {
        _enumStore.freeUnusedEnum(iter.getKey(), unused);
    }
    removeUnusedEnums(unused, cmp);
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::freeUnusedEnums(const IndexSet& toRemove,
                                                  const datastore::EntryComparator& cmp)
{
    IndexSet unused;
    for (const auto& index : toRemove) {
        _enumStore.freeUnusedEnum(index, unused);
    }
    removeUnusedEnums(unused, cmp);
}

template <typename DictionaryT>
bool
EnumStoreDictionary<DictionaryT>::findIndex(const datastore::EntryComparator& cmp,
                                            Index& idx) const
{
    auto itr = this->_dict.find(Index(), cmp);
    if (!itr.valid()) {
        return false;
    }
    idx = itr.getKey();
    return true;
}

template <typename DictionaryT>
bool
EnumStoreDictionary<DictionaryT>::findFrozenIndex(const datastore::EntryComparator& cmp,
                                                  Index& idx) const
{
    auto itr = this->_dict.getFrozenView().find(Index(), cmp);
    if (!itr.valid()) {
        return false;
    }
    idx = itr.getKey();
    return true;
}

template <typename DictionaryT>
std::vector<IEnumStore::EnumHandle>
EnumStoreDictionary<DictionaryT>::findMatchingEnums(const datastore::EntryComparator& cmp) const
{
    std::vector<IEnumStore::EnumHandle> result;
    auto itr = this->_dict.getFrozenView().find(Index(), cmp);
    while (itr.valid() && !cmp(Index(), itr.getKey())) {
        result.push_back(itr.getKey().ref());
        ++itr;
    }
    return result;
}

template <typename DictionaryT>
void
EnumStoreDictionary<DictionaryT>::onReset()
{
    this->_dict.clear();
}

template <>
EnumPostingTree &
EnumStoreDictionary<EnumTree>::getPostingDictionary()
{
    LOG_ABORT("should not be reached");
}

template <>
EnumPostingTree &
EnumStoreDictionary<EnumPostingTree>::getPostingDictionary()
{
    return _dict;
}

template <>
const EnumPostingTree &
EnumStoreDictionary<EnumTree>::getPostingDictionary() const
{
    LOG_ABORT("should not be reached");
}

template <>
const EnumPostingTree &
EnumStoreDictionary<EnumPostingTree>::getPostingDictionary() const
{
    return _dict;
}

template <typename DictionaryT>
bool
EnumStoreDictionary<DictionaryT>::hasData() const
{
    return DictionaryT::LeafNodeType::hasData();
}

EnumStoreFoldedDictionary::EnumStoreFoldedDictionary(IEnumStore& enumStore, std::unique_ptr<EntryComparator> folded_compare)
    : EnumStoreDictionary<EnumPostingTree>(enumStore),
      _folded_compare(std::move(folded_compare))
{
}

EnumStoreFoldedDictionary::~EnumStoreFoldedDictionary() = default;

UniqueStoreAddResult
EnumStoreFoldedDictionary::add(const EntryComparator &comp, std::function<EntryRef(void)> insertEntry)
{
    auto it = _dict.lowerBound(EntryRef(), comp);
    if (it.valid() && !comp(EntryRef(), it.getKey())) {
        // Entry already exists
        return UniqueStoreAddResult(it.getKey(), false);
    }
    EntryRef newRef = insertEntry();
    _dict.insert(it, newRef, EntryRef());
    // Maybe move posting list reference from next entry
    ++it;
    if (it.valid() && it.getData().valid() && !(*_folded_compare)(newRef, it.getKey())) {
        EntryRef posting_list_ref(it.getData());
        _dict.thaw(it);
        it.writeData(EntryRef());
        --it;
        assert(it.valid() && it.getKey() == newRef);
        it.writeData(posting_list_ref);
    }
    return UniqueStoreAddResult(newRef, true);
}

void
EnumStoreFoldedDictionary::remove(const EntryComparator &comp, EntryRef ref)
{
    assert(ref.valid());
    auto it = _dict.lowerBound(ref, comp);
    assert(it.valid() && it.getKey() == ref);
    EntryRef posting_list_ref(it.getData());
    _dict.remove(it);
    // Maybe copy posting list reference to next entry
    if (posting_list_ref.valid() && it.valid() && !it.getData().valid() && !(*_folded_compare)(ref, it.getKey())) {
        this->_dict.thaw(it);
        it.writeData(posting_list_ref);
    }
}

template class EnumStoreDictionary<EnumTree>;

template class EnumStoreDictionary<EnumPostingTree>;

template
class btree::BTreeNodeT<IEnumStore::Index, EnumTreeTraits::INTERNAL_SLOTS>;

template
class btree::BTreeNodeTT<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated, EnumTreeTraits::INTERNAL_SLOTS>;

template
class btree::BTreeNodeTT<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeInternalNode<IEnumStore::Index, btree::NoAggregated, EnumTreeTraits::INTERNAL_SLOTS>;

template
class btree::BTreeLeafNode<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeLeafNode<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeLeafNodeTemp<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeLeafNodeTemp<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeNodeStore<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                            EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeNodeStore<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                            EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeRoot<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                       const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTreeRoot<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                       const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTreeRootT<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                        const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTreeRootT<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                        const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTreeRootBase<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                           EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeRootBase<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                           EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeNodeAllocator<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                                EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeNodeAllocator<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                                EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS>;

template
class btree::BTreeIteratorBase<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                               EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS, EnumTreeTraits::PATH_SIZE>;
template
class btree::BTreeIteratorBase<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                               EnumTreeTraits::INTERNAL_SLOTS, EnumTreeTraits::LEAF_SLOTS, EnumTreeTraits::PATH_SIZE>;

template class btree::BTreeConstIterator<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                                         const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template class btree::BTreeConstIterator<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                                         const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTreeIterator<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                           const datastore::EntryComparatorWrapper, EnumTreeTraits>;
template
class btree::BTreeIterator<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                           const datastore::EntryComparatorWrapper, EnumTreeTraits>;

template
class btree::BTree<IEnumStore::Index, btree::BTreeNoLeafData, btree::NoAggregated,
                   const datastore::EntryComparatorWrapper, EnumTreeTraits>;
template
class btree::BTree<IEnumStore::Index, datastore::EntryRef, btree::NoAggregated,
                   const datastore::EntryComparatorWrapper, EnumTreeTraits>;

}
