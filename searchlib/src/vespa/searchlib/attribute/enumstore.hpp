// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "enumstore.h"
#include "enumcomparator.h"

#include <vespa/vespalib/util/exceptions.h>
#include <vespa/vespalib/util/hdr_abort.h>
#include <vespa/vespalib/btree/btreenode.hpp>
#include <vespa/vespalib/btree/btreenodestore.hpp>
#include <vespa/vespalib/btree/btreenodeallocator.hpp>
#include <vespa/vespalib/btree/btreeiterator.hpp>
#include <vespa/vespalib/btree/btreeroot.hpp>
#include <vespa/vespalib/btree/btreebuilder.hpp>
#include <vespa/vespalib/btree/btree.hpp>
#include <vespa/vespalib/datastore/unique_store.hpp>
#include <vespa/vespalib/datastore/unique_store_string_allocator.hpp>
#include <vespa/vespalib/util/array.hpp>
#include <vespa/vespalib/util/bufferwriter.h>
#include <vespa/searchcommon/common/compaction_strategy.h>

namespace search {

template <typename EntryType>
void EnumStoreT<EntryType>::freeUnusedEnum(Index idx, IndexSet& unused)
{
    const auto& entry = get_entry_base(idx);
    if (entry.get_ref_count() == 0) {
        unused.insert(idx);
        _store.get_allocator().hold(idx);
    }
}

template <typename EntryType>
ssize_t
EnumStoreT<EntryType>::load_unique_values_internal(const void* src,
                                                   size_t available,
                                                   IndexVector& idx)
{
    size_t left = available;
    const char* p = static_cast<const char*>(src);
    Index idx1;
    while (left > 0) {
        ssize_t sz = load_unique_value(p, left, idx1);
        if (sz < 0) {
            return sz;
        }
        p += sz;
        left -= sz;
        idx.push_back(idx1);
    }
    return available - left;
}

template <class EntryType>
ssize_t
EnumStoreT<EntryType>::load_unique_value(const void* src, size_t available, Index& idx)
{
    if (available < sizeof(DataType)) {
        return -1;
    }
    const auto* value = static_cast<const DataType*>(src);
    Index prev_idx = idx;
    idx = _store.get_allocator().allocate(*value);

    if (prev_idx.valid()) {
        assert(ComparatorType::compare(getValue(prev_idx), *value) < 0);
    }
    return sizeof(DataType);
}

template <typename EntryType>
EnumStoreT<EntryType>::EnumStoreT(bool has_postings)
    : _store(make_enum_store_dictionary(*this, has_postings, EntryType::hasFold() ? std::make_unique<FoldedComparatorType>(*this) : std::unique_ptr<datastore::EntryComparator>())),
      _dict(static_cast<IEnumStoreDictionary&>(_store.get_dictionary())),
      _cached_values_memory_usage(),
      _cached_values_address_space_usage(0, 0, (1ull << 32))
{
}

template <typename EntryType>
EnumStoreT<EntryType>::~EnumStoreT() = default;

template <typename EntryType>
vespalib::AddressSpace
EnumStoreT<EntryType>::getAddressSpaceUsage() const
{
    return _store.get_address_space_usage();
}

template <typename EntryType>
void
EnumStoreT<EntryType>::transferHoldLists(generation_t generation)
{
    _store.transferHoldLists(generation);
}

template <typename EntryType>
void
EnumStoreT<EntryType>::trimHoldLists(generation_t firstUsed)
{
    // remove generations in the range [0, firstUsed>
    _store.trimHoldLists(firstUsed);
}

template <typename EntryType>
ssize_t
EnumStoreT<EntryType>::load_unique_values(const void* src, size_t available, IndexVector& idx)
{
    ssize_t sz = load_unique_values_internal(src, available, idx);
    if (sz >= 0) {
        _dict.build(idx);
    }
    return sz;
}

template <typename EntryType>
bool
EnumStoreT<EntryType>::getValue(Index idx, DataType& value) const
{
    if (!idx.valid()) {
        return false;
    }
    value = _store.get(idx);
    return true;
}

template <typename EntryType>
EnumStoreT<EntryType>::NonEnumeratedLoader::~NonEnumeratedLoader() = default;

template <class EntryType>
void
EnumStoreT<EntryType>::writeValues(BufferWriter& writer, vespalib::ConstArrayRef<Index> idxs) const
{
    for (const auto& idx : idxs) {
        writer.write(&_store.get(idx), sizeof(DataType));
    }
}

template <class EntryType>
bool
EnumStoreT<EntryType>::foldedChange(const Index &idx1, const Index &idx2) const
{
    int cmpres = FoldedComparatorType::compareFolded(getValue(idx1), getValue(idx2));
    assert(cmpres <= 0);
    return cmpres < 0;
}

template <typename EntryType>
bool
EnumStoreT<EntryType>::findEnum(DataType value, IEnumStore::EnumHandle &e) const
{
    ComparatorType cmp(*this, value);
    Index idx;
    if (_dict.findFrozenIndex(cmp, idx)) {
        e = idx.ref();
        return true;
    }
    return false;
}

template <typename EntryType>
std::vector<IEnumStore::EnumHandle>
EnumStoreT<EntryType>::findFoldedEnums(DataType value) const
{
    FoldedComparatorType cmp(*this, value);
    return _dict.findMatchingEnums(cmp);
}

template <typename EntryType>
bool
EnumStoreT<EntryType>::findIndex(DataType value, Index &idx) const
{
    ComparatorType cmp(*this, value);
    return _dict.findIndex(cmp, idx);
}

template <typename EntryType>
void
EnumStoreT<EntryType>::freeUnusedEnums()
{
    ComparatorType cmp(*this);
    _dict.freeUnusedEnums(cmp);
}

template <typename EntryType>
void
EnumStoreT<EntryType>::freeUnusedEnums(const IndexSet& toRemove)
{
    ComparatorType cmp(*this);
    _dict.freeUnusedEnums(toRemove, cmp);
}

template <typename EntryType>
void
EnumStoreT<EntryType>::addEnum(DataType value, Index& newIdx)
{
    ComparatorType cmp(*this, value);
    auto add_result = _dict.add(cmp, [this, &value]() -> EntryRef { return _store.get_allocator().allocate(value); });
    newIdx = add_result.ref();
}

template <typename EntryType>
vespalib::MemoryUsage
EnumStoreT<EntryType>::update_stat()
{
    auto &store = _store.get_allocator().get_data_store();
    _cached_values_memory_usage = store.getMemoryUsage();
    _cached_values_address_space_usage = store.getAddressSpaceUsage();
    auto retval = _cached_values_memory_usage;
    retval.merge(_dict.get_memory_usage());
    return retval;
}

namespace {

// minimum dead bytes in enum store before consider compaction
constexpr size_t DEAD_BYTES_SLACK = 0x10000u;
constexpr size_t DEAD_ADDRESS_SPACE_SLACK = 0x10000u;

}
template <typename EntryType>
std::unique_ptr<IEnumStore::EnumIndexRemapper>
EnumStoreT<EntryType>::consider_compact(const CompactionStrategy& compaction_strategy)
{
    size_t used_bytes = _cached_values_memory_usage.usedBytes();
    size_t dead_bytes = _cached_values_memory_usage.deadBytes();
    size_t used_address_space = _cached_values_address_space_usage.used();
    size_t dead_address_space = _cached_values_address_space_usage.dead();
    bool compact_memory = ((dead_bytes >= DEAD_BYTES_SLACK) &&
                           (used_bytes * compaction_strategy.getMaxDeadBytesRatio() < dead_bytes));
    bool compact_address_space = ((dead_address_space >= DEAD_ADDRESS_SPACE_SLACK) &&
                                  (used_address_space * compaction_strategy.getMaxDeadAddressSpaceRatio() < dead_address_space));
    if (compact_memory || compact_address_space) {
        return compact_worst(compact_memory, compact_address_space);
    }
    return std::unique_ptr<IEnumStore::EnumIndexRemapper>();
}

template <typename EntryType>
std::unique_ptr<IEnumStore::EnumIndexRemapper>
EnumStoreT<EntryType>::compact_worst(bool compact_memory, bool compact_address_space)
{
    return _store.compact_worst(compact_memory, compact_address_space);
}

}
