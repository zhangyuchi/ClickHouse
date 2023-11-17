#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/AsynchronousReader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include "config.h"

namespace Poco { class Logger; }

namespace DB
{
class FilesystemCacheLog;

/**
 * Remote disk might need to split one clickhouse file into multiple files in remote fs.
 * This class works like a proxy to allow transition from one file into multiple.
 */
class ReadBufferFromRemoteFSGather final : public ReadBufferFromFileBase
{
friend class ReadIndirectBufferFromRemoteFS;

public:
    using ReadBufferCreator = std::function<std::unique_ptr<ReadBufferFromFileBase>(const std::string & path, size_t read_until_position)>;

    /// Supports both external buffer mode (when the caller always assigns internal_buffer before each
    /// nextImpl() call) and owned buffer mode. Automatically detects which one to use (lazily
    /// allocates own memory when nextImpl() is called with no buffer assigned).
    ReadBufferFromRemoteFSGather(
        ReadBufferCreator && read_buffer_creator_,
        const StoredObjects & blobs_to_read_,
        const ReadSettings & settings_,
        std::shared_ptr<FilesystemCacheLog> cache_log_);

    ~ReadBufferFromRemoteFSGather() override;

    String getFileName() const override { return current_object.remote_path; }

    String getInfoForLog() override { return current_buf ? current_buf->getInfoForLog() : ""; }

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override { return setReadUntilPosition(getFileSize()); }

    size_t getFileSize() override { return getTotalSize(blobs_to_read); }

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    off_t seek(off_t offset, int whence) override;

    off_t getPosition() override { return file_offset_of_buffer_end - available(); }

    bool isSeekCheap() override;

    bool isContentCached(size_t offset, size_t size) override;

private:
    SeekableReadBufferPtr createImplementationBuffer(const StoredObject & object);

    bool nextImpl() override;

    void initialize();

    bool readImpl();

    bool moveToNextBuffer();

    void appendUncachedReadInfo();

    void reset();

    /// If internal_buffer is empty, point it to own memory. Resets working_buffer.
    void ensureInternalBuffer();

    const ReadSettings settings;
    const StoredObjects blobs_to_read;
    const ReadBufferCreator read_buffer_creator;
    const std::shared_ptr<FilesystemCacheLog> cache_log;
    const String query_id;
    const bool with_cache;

    size_t read_until_position = 0;
    size_t file_offset_of_buffer_end = 0;

    StoredObject current_object;
    size_t current_buf_idx = 0;
    SeekableReadBufferPtr current_buf;

    LoggerPtr log;
};

size_t chooseBufferSizeForRemoteReading(const DB::ReadSettings & settings, size_t file_size);
}
