#include <Common/ConnectionPool.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/ProxyConfiguration.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/SipHash.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPChunkedStream.h>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>

#include "config.h"

#if USE_SSL
#include <Poco/Net/HTTPSClientSession.h>
#endif

Poco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB::ProxyConfiguration & proxy_configuration)
{
    Poco::Net::HTTPClientSession::ProxyConfig poco_proxy_config;

    poco_proxy_config.host = proxy_configuration.host;
    poco_proxy_config.port = proxy_configuration.port;
    poco_proxy_config.protocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.protocol);
    poco_proxy_config.tunnel = proxy_configuration.tunneling;
    poco_proxy_config.originalRequestProtocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.original_request_protocol);

    return poco_proxy_config;
}

size_t roundUp(size_t x, size_t rounding)
{
    return (x + (rounding - 1)) / rounding * rounding;
}

Poco::Timespan divide(const Poco::Timespan span, int divisor)
{
    return Poco::Timespan(Poco::Timestamp::TimeDiff(span.totalMicroseconds() / divisor));
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_URI_SCHEME;
}

namespace ProfileEvents
{
extern const Event S3StorageConnectionsCreated;
extern const Event S3StorageConnectionsReused;
extern const Event S3StorageConnectionsReset;
extern const Event S3StorageConnectionsPreserved;
extern const Event S3StorageConnectionsExpired;
extern const Event S3StorageConnectionsErrors;
extern const Event S3StorageConnectionsElapsedMicroseconds;

extern const Event S3DiskConnectionsCreated;
extern const Event S3DiskConnectionsReused;
extern const Event S3DiskConnectionsReset;
extern const Event S3DiskConnectionsPreserved;
extern const Event S3DiskConnectionsExpired;
extern const Event S3DiskConnectionsErrors;
extern const Event S3DiskConnectionsElapsedMicroseconds;

extern const Event HttpConnectionsCreated;
extern const Event HttpConnectionsReused;
extern const Event HttpConnectionsReset;
extern const Event HttpConnectionsPreserved;
extern const Event HttpConnectionsExpired;
extern const Event HttpConnectionsErrors;
extern const Event HttpConnectionsElapsedMicroseconds;
}

namespace CurrentMetrics
{
extern const Metric S3StorageConnectionsStored;
extern const Metric S3StorageConnectionsActive;

extern const Metric S3DiskConnectionsStored;
extern const Metric S3DiskConnectionsActive;

extern const Metric HttpConnectionsStored;
extern const Metric HttpConnectionsActive;
}

DB::ConnectionPoolMetrics getMetricsForS3StorageConnectionPool()
{
    return DB::ConnectionPoolMetrics{
        .created = ProfileEvents::S3StorageConnectionsCreated,
        .reused = ProfileEvents::S3StorageConnectionsReused,
        .reset = ProfileEvents::S3StorageConnectionsReset,
        .preserved = ProfileEvents::S3StorageConnectionsPreserved,
        .expired = ProfileEvents::S3StorageConnectionsExpired,
        .errors = ProfileEvents::S3StorageConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::S3StorageConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::S3StorageConnectionsStored,
        .active_count = CurrentMetrics::S3StorageConnectionsActive,
    };
}

DB::ConnectionPoolMetrics getMetricsForS3DiskConnectionPool()
{
    return DB::ConnectionPoolMetrics{
        .created = ProfileEvents::S3DiskConnectionsCreated,
        .reused = ProfileEvents::S3DiskConnectionsReused,
        .reset = ProfileEvents::S3DiskConnectionsReset,
        .preserved = ProfileEvents::S3DiskConnectionsPreserved,
        .expired = ProfileEvents::S3DiskConnectionsExpired,
        .errors = ProfileEvents::S3DiskConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::S3DiskConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::S3DiskConnectionsStored,
        .active_count = CurrentMetrics::S3DiskConnectionsActive,
    };
}

DB::ConnectionPoolMetrics getMetricsForHTTPConnectionPool()
{
    return DB::ConnectionPoolMetrics{
        .created = ProfileEvents::HttpConnectionsCreated,
        .reused = ProfileEvents::HttpConnectionsReused,
        .reset = ProfileEvents::HttpConnectionsReset,
        .preserved = ProfileEvents::HttpConnectionsPreserved,
        .expired = ProfileEvents::HttpConnectionsExpired,
        .errors = ProfileEvents::HttpConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::HttpConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::HttpConnectionsStored,
        .active_count = CurrentMetrics::HttpConnectionsActive,
    };
}

DB::ConnectionPoolMetrics DB::IEndpointConnectionPool::getMetrics(DB::MetricsType type)
{
    switch (type)
    {
        case DB::MetricsType::METRICS_FOR_S3_STORAGE:
            return getMetricsForS3StorageConnectionPool();
        case DB::MetricsType::METRICS_FOR_S3_DISK:
            return getMetricsForS3DiskConnectionPool();
        case DB::MetricsType::METRICS_FOR_HTTP:
            return getMetricsForHTTPConnectionPool();
    }
}


// That classes manage connections to the endpoint
// Features:
// - it uses HostResolvePool for address selecting. See Common/HostResolvePool.h for more info.
// - it minimizes number of `Session::connect()`/`Session::reconnect()` calls
//   - stores only connected and ready to use sessions
//   - connection could be reused even when limits are reached
// - soft limit
// - warn limit
// - `Session::reconnect()` uses the pool as well
// - comprehensive sensors
// - session is reused according its inner state, automatically


// connection live stages
// CREATED -> STORED
// CREATED -> RESET
// STORED -> EXPIRED
// STORED -> REUSED
// REUSED -> RESET
// REUSED -> STORED


template<class Session>
class EndpointConnectionPool : public std::enable_shared_from_this<EndpointConnectionPool<Session>>, public DB::IEndpointConnectionPool
{
private:
    using WeakPtr = std::weak_ptr<EndpointConnectionPool<Session>>;

    class PooledConnection : public Session
    {
    public:
        using Ptr = std::shared_ptr<PooledConnection>;

        void reconnect() override
        {
            LOG_TEST(log, "reconnected : {}", getTarget());

            ProfileEvents::increment(metrics.reset);
            Session::close();

            if (auto lock = pool.lock())
            {
                LOG_TEST(log, "reconnected by pool: {}", getTarget());
                auto timeouts = DB::getTimeouts(*this);
                auto new_connection = lock->getConnection(timeouts);
                jumpToOtherConnection(dynamic_cast<PooledConnection &>(*new_connection));
                //new_connection->reset();
            }
            else
            {
                LOG_TEST(log, "reconnected without pool: {}", getTarget());
                ProfileEvents::increment(metrics.created);
                Session::reconnect();
            }
        }

        String getTarget() const
        {
            if (!Session::getProxyConfig().host.empty())
                return fmt::format("{} over proxy {}", Session::getHost(), Session::getProxyConfig().host);
            return Session::getHost();
        }

        void flushRequest() override
        {
            if (request_stream)
            {
                request_stream->flush();

                if (auto * fixed_steam = dynamic_cast<Poco::Net::HTTPFixedLengthOutputStream *>(request_stream))
                {
                    LOG_TEST(log, "HTTPFixedLengthOutputStream");
                    request_stream_completed = fixed_steam->isComplete();
                }
                else if (auto * chunked_steam = dynamic_cast<Poco::Net::HTTPChunkedOutputStream *>(request_stream))
                {
                    LOG_TEST(log, "HTTPChunkedOutputStream");
                    chunked_steam->rdbuf()->close();
                    request_stream_completed = chunked_steam->isComplete();
                }
                else if (auto * http_stream = dynamic_cast<Poco::Net::HTTPOutputStream *>(request_stream))
                {
                    LOG_TEST(log, "HTTPOutputStream");
                    request_stream_completed = http_stream->isComplete();
                }
                else
                {
                    LOG_TEST(log, "calculateComplete OUT stream is unknown");
                    request_stream_completed = false;
                }
            }
            request_stream = nullptr;
            LOG_TEST(log, "flushing request OUT request_stream_completed : {}", request_stream_completed);


            Session::flushRequest();
        }

        std::ostream & sendRequest(Poco::Net::HTTPRequest & request) override
        {
            LOG_TEST(log, "session send request");

            for (const auto & header : request)
            {
                LOG_TEST(log, "headers {}: {}", header.first, header.second);
            }

            std::ostream & result = Session::sendRequest(request);
            result.exceptions(std::ios::badbit);

            request_stream = &result;
            request_stream_completed = false;

            response_stream = nullptr;
            response_stream_completed = true;

            return result;
        }

        std::istream & receiveResponse(Poco::Net::HTTPResponse & response) override
        {
            LOG_TEST(log, "session receive response");

            std::istream & result = Session::receiveResponse(response);
            result.exceptions(std::ios::badbit);

            LOG_TEST(log, "status: {} {} ver: {} keep-alive: {}",
                     int(response.getStatus()), response.getReason(), response.getVersion(), response.getKeepAlive());

            for (const auto & header : response)
            {
                LOG_TEST(log, "headers: {} - {}", header.first, header.second);
            }


            response_stream = &result;
            response_stream_completed = false;

            return result;
        }

        void reset() override
        {
            request_stream = nullptr;
            request_stream_completed = false;

            response_stream = nullptr;
            response_stream_completed = false;

            Session::reset();
        }

        ~PooledConnection() override
        {
            LOG_TEST(log, "flushing response IN {}", bool(response_stream));
            if (response_stream)
            {
                if (auto * fixed_steam = dynamic_cast<Poco::Net::HTTPFixedLengthInputStream *>(response_stream))
                {
                    LOG_TEST(log, "HTTPFixedLengthInputStream");
                    response_stream_completed = fixed_steam->isComplete();
                }
                else if (auto * chunked_steam = dynamic_cast<Poco::Net::HTTPChunkedInputStream *>(response_stream))
                {
                    LOG_TEST(log, "HTTPChunkedInputStream");
                    response_stream_completed = chunked_steam->isComplete();
                }
                else if (auto * http_stream = dynamic_cast<Poco::Net::HTTPInputStream *>(response_stream))
                {
                    LOG_TEST(log, "HTTPInputStream");
                    response_stream_completed = http_stream->isComplete();
                }
                else
                {
                    LOG_TEST(log, "calculateComplete IN stream is unknown");
                    response_stream_completed = false;
                }
            }
            response_stream = nullptr;
            LOG_TEST(log, "flushing request IN {} : {}", bool(response_stream), response_stream_completed);

            LOG_TEST(log, "~Sessions {}. request_completed {}", Session::getHost(), request_stream_completed && response_stream_completed);

            if (auto lock = pool.lock())
            {
                --lock->active_connections;
                lock->storeDestroyingConnection(*this);
            }
            else
            {
                ProfileEvents::increment(metrics.reset);
            }

            CurrentMetrics::sub(metrics.active_count);
        }

    private:
        friend class EndpointConnectionPool;

        template<class... Args>
        explicit PooledConnection(EndpointConnectionPool & pool_, Args&&... args)
            : Session(args...)
            , pool(pool_.getWeakFromThis())
            , metrics(pool_.metrics)
        {
            ++pool_.active_connections;
            CurrentMetrics::add(metrics.active_count);
        }

        template<class... Args>
        static Ptr create(Args&&... args)
        {
            struct make_shared_enabler : public PooledConnection
            {
                explicit make_shared_enabler(Args&&... args) : PooledConnection(args...) {}
            };
            return std::make_shared<make_shared_enabler>(args...);
        }

        void doConnect()
        {
            Session::reconnect();
        }

        void jumpToOtherConnection(PooledConnection & connection)
        {
            chassert(this != &connection);

            auto timeouts = DB::getTimeouts(connection);

            auto buffer = Poco::Buffer<char>(0);
            connection.drainBuffer(buffer);
            /// may be it should be like drop this connection and take another from pool
            if (!buffer.empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "jumpToOtherConnection with buffered data in dst");

            Session::drainBuffer(buffer);

            if (Session::getHost() != connection.getHost())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "jumpToOtherConnection with different host");

            Session::attachSocket(connection.detachSocket());
            Session::setLastRequest(connection.getLastRequest());
            Session::setResolvedHost(connection.getResolvedHost());
            Session::setKeepAlive(connection.getKeepAlive());
            setTimeouts(*this, timeouts);

            if (!connection.getProxyConfig().host.empty())
                Session::setProxyConfig(connection.getProxyConfig());

            connection.reset();
        }

        bool isCompleted() const
        {
            return request_stream_completed && response_stream_completed;
        }

        WeakPtr pool;
        DB::ConnectionPoolMetrics metrics;

        Poco::Logger * log = &Poco::Logger::get("PooledConnection");

        std::ostream * request_stream = nullptr;
        std::istream * response_stream = nullptr;

        bool request_stream_completed = true;
        bool response_stream_completed = true;
    };

    using Connection = PooledConnection;
    using ConnectionPtr = PooledConnection::Ptr;

    struct GreaterByLastRequest
    {
        static bool operator() (const ConnectionPtr & l, const ConnectionPtr & r)
        {
            return l->getLastRequest() > r->getLastRequest();
        }
    };

    using ConnectionsMinHeap = std::priority_queue<ConnectionPtr, std::vector<ConnectionPtr>, GreaterByLastRequest>;

public:
    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public EndpointConnectionPool<Session>
        {
            explicit make_shared_enabler(Args&&... args) : EndpointConnectionPool<Session>(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    ~EndpointConnectionPool() override
    {
        std::lock_guard lock(mutex);
        CurrentMetrics::sub(metrics.stored_count, stored_connections.size());
    }

    String getTarget() const
    {
        if (!proxy_configuration.isEmpty())
            return fmt::format("{} over proxy {}", host, proxy_configuration.host);
        return host;
    }

    void dropResolvedHostsCache() override
    {
            resolve_pool->reset();
    }

    IEndpointConnectionPool::ConnectionPtr getConnection(const DB::ConnectionTimeouts & timeouts) override
    {
        size_t reused = 0;
        /// upd stats outsize of critical section
        SCOPE_EXIT({
            ProfileEvents::increment(metrics.reused, reused);
            CurrentMetrics::sub(metrics.stored_count, reused);
        });

        wipeExpired();

        {
            std::lock_guard lock(mutex);
            if (!stored_connections.empty())
            {
                auto it = stored_connections.top();
                stored_connections.pop();
                ++reused;
                setTimeouts(*it, timeouts);
                LOG_TEST(log, "reused {}", getTarget());
                return it;
            }
        }

        return prepareNewConnection(timeouts);
    }

private:
    EndpointConnectionPool(
        String host_,
        UInt16 port_,
        bool https_,
        DB::ProxyConfiguration proxy_configuration_,
        DB::MetricsType metrics_type,
        DB::ConnectionPoolLimits limits_)
    : host(std::move(host_))
    , port(port_)
    , https(https_)
    , proxy_configuration(std::move(proxy_configuration_))
    , metrics(DB::IEndpointConnectionPool::getMetrics(metrics_type))
    , limits(limits_)
    , resolve_pool(DB::HostResolvePool::create(host, metrics_type))
    {
    }

    WeakPtr getWeakFromThis()
    {
        return EndpointConnectionPool::weak_from_this();
    }

    bool isExpired(Poco::Timestamp & now, ConnectionPtr connection) TSA_REQUIRES(mutex)
    {
        if (stored_connections.size() > limits.soft_limit)
            return now > (connection->getLastRequest() + divide(connection->getKeepAliveTimeout(), 10));
        return now > connection->getLastRequest() + connection->getKeepAliveTimeout();
    }

    void wipeExpired()
    {
        size_t expired = 0;

        /// upd stats outsize of critical section
        SCOPE_EXIT({
            CurrentMetrics::sub(metrics.stored_count, expired);
            ProfileEvents::increment(metrics.expired, expired);
        });

        Poco::Timestamp now;

        std::lock_guard lock(mutex);
        while (!stored_connections.empty())
        {
            auto connection = stored_connections.top();
            if (!isExpired(now, connection))
                return;

            stored_connections.pop();
            connection->reset();
            LOG_TEST(log, "expired {}", getTarget());
            ++expired;
        }
    }

    ConnectionPtr allocateNewConnection()
    {
        /// Pool is global, we shouldn't attribute this memory to query/user.
        DB::MemoryTrackerSwitcher switcher{&total_memory_tracker};

        ConnectionPtr connection = PooledConnection::create(*this, host, port);
        connection->setKeepAlive(true);

        if (!proxy_configuration.isEmpty())
            connection->setProxyConfig(proxyConfigurationToPocoProxyConfig(proxy_configuration));

        return connection;
    }

    ConnectionPtr prepareNewConnection(const DB::ConnectionTimeouts & timeouts)
    {
        auto address = resolve_pool->get();

        auto active_sessions_ = active_connections.load();
        auto mute_warn_until_ = mute_warn_until.load();

        if (active_sessions_ >= limits.warning_limit && active_sessions_ >= mute_warn_until_)
        {
            LOG_WARNING(log, "Too many active sessions for the host {}, count {}", getTarget(), active_sessions_);
            mute_warn_until.store(roundUp(active_sessions_, 100));
        }

        if (active_sessions_ < limits.warning_limit && mute_warn_until_ > 0)
        {
            LOG_WARNING(log, "Sessions count is OK for the host {}, count {}", getTarget(), active_sessions_);
            mute_warn_until.store(0);
        }

        auto session = allocateNewConnection();

        setTimeouts(*session, timeouts);
        session->setResolvedHost(*address);

        try {
            auto timer = DB::CurrentThread::getProfileEvents().timer(metrics.elapsed_microseconds);
            session->doConnect();
        }
        catch (...)
        {
            address.setFail();
            ProfileEvents::increment(metrics.errors);
            session->reset();
            LOG_TEST(log, "new session failed {} {}, conn timeout {}ms",
                     active_connections, getTarget(), timeouts.connection_timeout.totalMilliseconds());
            throw;
        }

        LOG_TEST(log, "new session created {} {}", active_connections, getTarget());
        ProfileEvents::increment(metrics.created);
        return session;
    }

    void storeDestroyingConnection(PooledConnection & connection)
    {
        size_t preserved = 0;

        /// upd stats outsize of critical section
        SCOPE_EXIT({
            CurrentMetrics::add(metrics.stored_count, preserved);
            ProfileEvents::increment(metrics.preserved, preserved);
            ProfileEvents::increment(metrics.reset, 1 - preserved);
        });

        if (!connection.connected())
        {
            LOG_TEST(log, "reset !connected {}", getTarget());
            return;
        }

        if (connection.mustReconnect())
        {
            LOG_TEST(log, "reset mustReconnect {}, kat {}ms, age {}ms",
                     getTarget(), connection.getKeepAliveTimeout().totalMilliseconds(), (Poco::Timestamp() - connection.getLastRequest()) / 1000);
            return;
        }

        if (!connection.isCompleted())
        {
            LOG_TEST(log, "reset !isCompleted {}", getTarget());
            return;
        }

        if (connection.buffered())
        {
            LOG_TEST(log, "reset buffered {}", getTarget());
            return;
        }

        {
            std::lock_guard lock(mutex);

            auto connection_to_store = allocateNewConnection();
            connection_to_store->jumpToOtherConnection(connection);
            stored_connections.push(connection_to_store);
            ++preserved;
        }
        LOG_TEST(log, "preserved {}", getTarget());
    }

    const std::string host;
    const UInt16 port;
    const bool https;
    const DB::ProxyConfiguration proxy_configuration;

    DB::ConnectionPoolMetrics metrics;
    DB::ConnectionPoolLimits limits;

    Poco::Logger * log = &Poco::Logger::get("ConnectionPool");

    DB::HostResolvePool::Ptr resolve_pool;

    std::mutex mutex;
    ConnectionsMinHeap stored_connections TSA_GUARDED_BY(mutex);

    std::atomic<size_t> active_connections = 0;
    std::atomic<size_t> mute_warn_until = 0;
};

DB::IEndpointConnectionPool::Ptr createHttpConnectionPool(
    std::string host,
    UInt16 port,
    bool secure,
    DB::ProxyConfiguration proxy_configuration,
    DB:: ConnectionPoolLimits limits)
{
    if (secure)
    {
#if USE_SSL
        return EndpointConnectionPool<Poco::Net::HTTPSClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_HTTP, std::move(limits));
#else
        throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                        "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
#endif
    }
    else
    {
        return EndpointConnectionPool<Poco::Net::HTTPClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_HTTP, std::move(limits));
    }
}

DB::IEndpointConnectionPool::Ptr createS3StorageConnectionPool(
    std::string host,
    UInt16 port,
    bool secure,
    DB::ProxyConfiguration proxy_configuration,
    DB::ConnectionPoolLimits limits)
{
    if (secure)
    {
#if USE_SSL
        return EndpointConnectionPool<Poco::Net::HTTPSClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_S3_STORAGE, std::move(limits));
#else
        throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                        "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
#endif
    }
    else
    {
        return EndpointConnectionPool<Poco::Net::HTTPClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_S3_STORAGE, std::move(limits));
    }
}

DB::IEndpointConnectionPool::Ptr createS3DiskConnectionPool(
    std::string host,
    UInt16 port,
    bool secure,
    DB::ProxyConfiguration proxy_configuration,
    DB::ConnectionPoolLimits limits)
{
    if (secure)
    {
#if USE_SSL
        return EndpointConnectionPool<Poco::Net::HTTPSClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_S3_DISK, std::move(limits));
#else
        throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                        "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
#endif
    }
    else
    {
        return EndpointConnectionPool<Poco::Net::HTTPClientSession>::create(
            std::move(host), port, secure, std::move(proxy_configuration), DB::MetricsType::METRICS_FOR_S3_DISK, std::move(limits));
    }
}


bool DB::ConnectionPools::EndpointPoolKey::operator==(const DB::ConnectionPools::EndpointPoolKey & rhs) const
{
    return std::tie(
               target_host,
               target_port,
               is_target_https,
               proxy_config.host,
               proxy_config.port,
               proxy_config.protocol,
               proxy_config.tunneling,
               proxy_config.original_request_protocol)
        == std::tie(
               rhs.target_host,
               rhs.target_port,
               rhs.is_target_https,
               rhs.proxy_config.host,
               rhs.proxy_config.port,
               rhs.proxy_config.protocol,
               rhs.proxy_config.tunneling,
               rhs.proxy_config.original_request_protocol);
}

size_t DB::ConnectionPools::Hasher::operator()(const DB::ConnectionPools::EndpointPoolKey & k) const
{
    SipHash s;
    s.update(k.target_host);
    s.update(k.target_port);
    s.update(k.is_target_https);
    s.update(k.proxy_config.host);
    s.update(k.proxy_config.port);
    s.update(k.proxy_config.protocol);
    s.update(k.proxy_config.tunneling);
    s.update(k.proxy_config.original_request_protocol);
    return s.get64();
}

bool DB::ConnectionPools::declarePoolForS3Storage(
    const Poco::URI & uri, DB::ProxyConfiguration proxy_configuration, DB::ConnectionPoolLimits limits)
{
    auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
    auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

    std::unique_lock lock(mutex);
    return endpoints_pool.emplace(
        key,
        createS3StorageConnectionPool(
            std::move(host),
            port,
            secure,
            std::move(proxy_configuration),
            std::move(limits))).second;
}

bool DB::ConnectionPools::declarePoolForS3Disk(
    const Poco::URI & uri, DB::ProxyConfiguration proxy_configuration, DB::ConnectionPoolLimits limits)
{
    auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
    auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

    std::unique_lock lock(mutex);
    return endpoints_pool.emplace(
        key,
        createS3DiskConnectionPool(
            std::move(host),
            port,
            secure,
            std::move(proxy_configuration),
            std::move(limits))).second;
}

bool DB::ConnectionPools::declarePoolForHttp(
    const Poco::URI & uri, DB::ProxyConfiguration proxy_configuration, DB::ConnectionPoolLimits limits)
{
    auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
    auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

    std::unique_lock lock(mutex);
    return endpoints_pool.emplace(
        key,
        createS3DiskConnectionPool(
            std::move(host),
            port,
            secure,
            std::move(proxy_configuration),
            std::move(limits))).second;
}

bool DB::ConnectionPools::isPoolDeclared(const Poco::URI & uri, DB::ProxyConfiguration proxy_configuration)
{
    auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
    auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

    std::unique_lock lock(mutex);
    auto it = endpoints_pool.find(key);
    return it != endpoints_pool.end();
}

DB::IEndpointConnectionPool::Ptr DB::ConnectionPools::getPool(const Poco::URI & uri, DB::ProxyConfiguration proxy_configuration)
{
    auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
    auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

    std::unique_lock lock(mutex);
    auto it = endpoints_pool.find(key);
    if (it == endpoints_pool.end())
    {
        auto[inserted_it, _] = endpoints_pool.emplace(
            key,
            createHttpConnectionPool(
                std::move(host),
                port,
                secure,
                std::move(proxy_configuration),
                ConnectionPoolLimits())); // get it from global settings

        return inserted_it->second;
    }

    return it->second;
}

bool DB::ConnectionPools::useSecureConnection(
    const Poco::URI & uri, const DB::ProxyConfiguration & proxy_configuration)
{
    if (uri.getScheme() == "http")
        return false;

    if (uri.getScheme() != "https")
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED_URI_SCHEME, "Unsupported scheme in URI '{}'", uri.toString());

    if (!proxy_configuration.isEmpty())
    {
        if (DB::ProxyConfiguration::Protocol::HTTP == proxy_configuration.protocol && !proxy_configuration.tunneling)
        {
            // If it is an HTTPS request, proxy server is HTTP and user opted for tunneling off, we must not create an HTTPS request.
            // The desired flow is: HTTP request to the proxy server, then proxy server will initiate an HTTPS request to the target server.
            // There is a weak link in the security, but that's what the user opted for.
            return false;
        }
    }

    return true;
}

std::tuple<std::string, UInt16, bool>
    DB::ConnectionPools::getHostPortSecure(
        const Poco::URI & uri, const DB::ProxyConfiguration & proxy_configuration)
{
    return std::make_tuple(uri.getHost(), uri.getPort(), useSecureConnection(uri, proxy_configuration));
}

void DB::ConnectionPools::dropResolvedHostsCache()
{
    std::vector<IEndpointConnectionPool::Ptr> gathered_pools;

    {
        std::unique_lock lock(mutex);

        gathered_pools.reserve(endpoints_pool.size());
        for (auto & [_, pool] : endpoints_pool)
        {
            gathered_pools.push_back(pool);
        }
    }

    for (auto & pool : gathered_pools)
    {
        pool->dropResolvedHostsCache();
    }
}


void DB::ConnectionPools::dropConnectionsCache()
{
    /// TODO: drop connections inside pools not all pools
    std::unique_lock lock(mutex);
    endpoints_pool.clear();
}
