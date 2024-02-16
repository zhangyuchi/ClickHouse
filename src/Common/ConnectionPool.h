#pragma once

#include <IO/ConnectionTimeouts.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProxyConfiguration.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Timespan.h>
#include <Poco/Net/HTTPClientSession.h>

#include <mutex>
#include <memory>

namespace DB
{

enum class ConnectionGroupType
{
    DISK,
    STORAGE,
    HTTP,
};

struct ConnectionPoolMetrics
{
    const ProfileEvents::Event created = ProfileEvents::end();
    const ProfileEvents::Event reused = ProfileEvents::end();
    const ProfileEvents::Event reset = ProfileEvents::end();
    const ProfileEvents::Event preserved = ProfileEvents::end();
    const ProfileEvents::Event expired = ProfileEvents::end();
    const ProfileEvents::Event errors = ProfileEvents::end();
    const ProfileEvents::Event elapsed_microseconds = ProfileEvents::end();

    const CurrentMetrics::Metric stored_count = CurrentMetrics::end();
    const CurrentMetrics::Metric active_count = CurrentMetrics::end();
};

struct ConnectionPoolLimits
{
    size_t soft_limit = 100;
    size_t warning_limit = 1000;
    size_t hard_limit = 10000;
};

class IEndpointConnectionPool
{
public:
    using Ptr =  std::shared_ptr<IEndpointConnectionPool>;
    using Connection = Poco::Net::HTTPClientSession;
    using ConnectionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

    /// can throw Poco::Net::Exception, DB::NetException, DB::Exception
    virtual ConnectionPtr getConnection(const ConnectionTimeouts & timeouts) = 0;
    virtual ConnectionPoolMetrics getMetrics() const = 0;
    virtual ~IEndpointConnectionPool() = default;

protected:
    IEndpointConnectionPool() = default;

    IEndpointConnectionPool(const IEndpointConnectionPool &) = delete;
    IEndpointConnectionPool & operator=(const IEndpointConnectionPool &) = delete;
};

class ConnectionPools
{
private:
    ConnectionPools();
    ConnectionPools(const ConnectionPools &) = delete;
    ConnectionPools & operator=(const ConnectionPools &) = delete;

public:
    static ConnectionPools & instance();

    void setLimits(ConnectionPoolLimits disk, ConnectionPoolLimits storage, ConnectionPoolLimits http);
    void dropResolvedHostsCache();
    void dropConnectionsCache();

    IEndpointConnectionPool::Ptr getPool(ConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
