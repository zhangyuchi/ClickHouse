#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "ddl_worker_queue" with list of queries that are currently in the DDL worker queue.
  */
class StorageSystemDDLWorkerQueue final : public ext::shared_ptr_helper<StorageSystemDDLWorkerQueue>,
                                          public IStorageSystemOneBlock<StorageSystemDDLWorkerQueue>
{
    friend struct ext::shared_ptr_helper<StorageSystemDDLWorkerQueue>;

protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDDLWorkerQueue"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
