#pragma once

#include <IO/ConnectionTimeouts.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProxyConfiguration.h>
#include <Common/HostResolvePool.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Timespan.h>
#include <Poco/Net/HTTPClientSession.h>

#include <mutex>
#include <memory>

namespace DB
{

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
    const size_t soft_limit = 1000;
    const size_t warning_limit = 20000;
};

class IEndpointConnectionPool
{
public:
    using Ptr =  std::shared_ptr<IEndpointConnectionPool>;
    using Connection = Poco::Net::HTTPClientSession;
    using ConnectionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

    IEndpointConnectionPool(const IEndpointConnectionPool &) = delete;
    IEndpointConnectionPool & operator=(const IEndpointConnectionPool &) = delete;

    /// can throw Poco::Net::Exception, DB::NetException, DB::Exception
    virtual ConnectionPtr getConnection(const ConnectionTimeouts & timeouts) = 0;
    virtual void dropResolvedHostsCache() = 0;
    virtual ~IEndpointConnectionPool() = default;

    static ConnectionPoolMetrics getMetrics(DB::MetricsType type);

protected:
    IEndpointConnectionPool() = default;
};


class ConnectionPools
{
public:
    struct EndpointPoolKey
    {
        String target_host;
        UInt16 target_port;
        bool is_target_https;
        ProxyConfiguration proxy_config;
        bool operator ==(const EndpointPoolKey & rhs) const;
    };

private:
    struct Hasher
    {
        size_t operator()(const EndpointPoolKey & k) const;
    };

    std::mutex mutex;
    std::unordered_map<EndpointPoolKey, IEndpointConnectionPool::Ptr, Hasher> endpoints_pool;

protected:
    ConnectionPools() = default;

public:
    ConnectionPools(const ConnectionPools &) = delete;
    ConnectionPools & operator=(const ConnectionPools &) = delete;

    static ConnectionPools & instance()
    {
        static ConnectionPools instance;
        return instance;
    }

    bool declarePoolForS3Storage(const Poco::URI & uri, ProxyConfiguration proxy_configuration, ConnectionPoolLimits limits);
    bool declarePoolForS3Disk(const Poco::URI & uri, ProxyConfiguration proxy_configuration, ConnectionPoolLimits limits);
    bool declarePoolForHttp(const Poco::URI & uri, ProxyConfiguration proxy_configuration, ConnectionPoolLimits limits);
    bool isPoolDeclared(const Poco::URI & uri, ProxyConfiguration proxy_configuration);
    IEndpointConnectionPool::Ptr getPool(const Poco::URI & uri, ProxyConfiguration proxy_configuration);

    void dropResolvedHostsCache();
    void dropConnectionsCache();

protected:
    static bool useSecureConnection(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration);
    static std::tuple<std::string, UInt16, bool> getHostPortSecure(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration);
};

}
