#include <Common/ConnectionPool.h>
#include <Common/HostResolvePool.h>

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
#include <Poco/Timespan.h>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>

#include "config.h"

#if USE_SSL
#include <Poco/Net/HTTPSClientSession.h>
#endif


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_URI_SCHEME;
}


namespace ProfileEvents
{
    extern const Event StorageConnectionsCreated;
    extern const Event StorageConnectionsReused;
    extern const Event StorageConnectionsReset;
    extern const Event StorageConnectionsPreserved;
    extern const Event StorageConnectionsExpired;
    extern const Event StorageConnectionsErrors;
    extern const Event StorageConnectionsElapsedMicroseconds;

    extern const Event DiskConnectionsCreated;
    extern const Event DiskConnectionsReused;
    extern const Event DiskConnectionsReset;
    extern const Event DiskConnectionsPreserved;
    extern const Event DiskConnectionsExpired;
    extern const Event DiskConnectionsErrors;
    extern const Event DiskConnectionsElapsedMicroseconds;

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
    extern const Metric StorageConnectionsStored;
    extern const Metric StorageConnectionsActive;

    extern const Metric DiskConnectionsStored;
    extern const Metric DiskConnectionsActive;

    extern const Metric HttpConnectionsStored;
    extern const Metric HttpConnectionsActive;
}


namespace
{
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
        chassert(rounding > 0);
        return (x + (rounding - 1)) / rounding * rounding;
    }


    Poco::Timespan divide(const Poco::Timespan span, int divisor)
    {
        return Poco::Timespan(Poco::Timestamp::TimeDiff(span.totalMicroseconds() / divisor));
    }


    DB::ConnectionPoolMetrics getMetricsForS3StorageConnectionPool()
    {
        return DB::ConnectionPoolMetrics{
            .created = ProfileEvents::StorageConnectionsCreated,
            .reused = ProfileEvents::StorageConnectionsReused,
            .reset = ProfileEvents::StorageConnectionsReset,
            .preserved = ProfileEvents::StorageConnectionsPreserved,
            .expired = ProfileEvents::StorageConnectionsExpired,
            .errors = ProfileEvents::StorageConnectionsErrors,
            .elapsed_microseconds = ProfileEvents::StorageConnectionsElapsedMicroseconds,
            .stored_count = CurrentMetrics::StorageConnectionsStored,
            .active_count = CurrentMetrics::StorageConnectionsActive,
        };
    }


    DB::ConnectionPoolMetrics getMetricsForS3DiskConnectionPool()
    {
        return DB::ConnectionPoolMetrics{
            .created = ProfileEvents::DiskConnectionsCreated,
            .reused = ProfileEvents::DiskConnectionsReused,
            .reset = ProfileEvents::DiskConnectionsReset,
            .preserved = ProfileEvents::DiskConnectionsPreserved,
            .expired = ProfileEvents::DiskConnectionsExpired,
            .errors = ProfileEvents::DiskConnectionsErrors,
            .elapsed_microseconds = ProfileEvents::DiskConnectionsElapsedMicroseconds,
            .stored_count = CurrentMetrics::DiskConnectionsStored,
            .active_count = CurrentMetrics::DiskConnectionsActive,
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


    DB::ConnectionPoolMetrics getConnectionPoolMetrics(DB::ConnectionGroupType type)
    {
        switch (type)
        {
            case DB::ConnectionGroupType::STORAGE:
                return getMetricsForS3StorageConnectionPool();
            case DB::ConnectionGroupType::DISK:
                return getMetricsForS3DiskConnectionPool();
            case DB::ConnectionGroupType::HTTP:
                return getMetricsForHTTPConnectionPool();
        }
    }


    class ConnectionGroup
    {
    public:
        using Ptr = std::shared_ptr<ConnectionGroup>;

        explicit ConnectionGroup(DB::ConnectionGroupType type_)
            : type(type_)
            , metrics(getConnectionPoolMetrics(type_))
        {}

        void setLimits(DB::ConnectionPoolLimits limits_)
        {
            std::lock_guard lock(mutex);
            limits = std::move(limits_);
            mute_warning_until = 0;
        }

        bool isSoftLimitReached() const
        {
            std::lock_guard lock(mutex);
            return active_connections_in_group >= limits.soft_limit;
        }

        bool isHardLimitReached() const
        {
            std::lock_guard lock(mutex);
            return active_connections_in_group >= limits.hard_limit;
        }

        void atConnectionCreate()
        {
            std::lock_guard lock(mutex);

            ++active_connections_in_group;

            if (active_connections_in_group >= limits.warning_limit && active_connections_in_group >= mute_warning_until)
            {
                LOG_WARNING(log, "Too many active sessions in group {}, count {}", type, active_connections_in_group);
                mute_warning_until = roundUp(active_connections_in_group, 100);
            }
        }

        void atConnectionDestroy()
        {
            std::lock_guard lock(mutex);

            --active_connections_in_group;

            if (active_connections_in_group < limits.warning_limit && mute_warning_until > 0)
            {
                LOG_WARNING(log, "Sessions count is OK in the group {}, count {}", type, active_connections_in_group);
                mute_warning_until = 0;
            }
        }

        void addConnections(size_t num)
        {
            std::lock_guard lock(mutex);
            active_connections_in_group += num;
        }

        void subConnections(size_t num)
        {
            std::lock_guard lock(mutex);
            if (num > active_connections_in_group)
                active_connections_in_group -= num;
            else
                active_connections_in_group = 0;
        }

        DB::ConnectionGroupType getType() const
        {
            return type;
        }

        const DB::ConnectionPoolMetrics & getMetrics() const
        {
            return metrics;
        }

    private:

        const DB::ConnectionGroupType type;
        const DB::ConnectionPoolMetrics metrics;

        LoggerPtr log = ::getLogger("ConnectionGroup");

        mutable std::mutex mutex;
        DB::ConnectionPoolLimits limits TSA_GUARDED_BY(mutex) = DB::ConnectionPoolLimits();
        size_t active_connections_in_group TSA_GUARDED_BY(mutex) = 0;
        size_t mute_warning_until TSA_GUARDED_BY(mutex) = 0;
    };


    class IExtendedPool : public DB::IEndpointConnectionPool
    {
    public:
        using Ptr = std::shared_ptr<IExtendedPool>;

        virtual DB::ConnectionGroupType getGroupType() const = 0;
        virtual void setGroup(ConnectionGroup::Ptr new_group) = 0;
        virtual void dropResolvedHostsCache() = 0;
        virtual size_t wipeExpired() = 0;
    };


    // EndpointConnectionPool manage connections to the endpoint
    // Features:
    // - it uses HostResolvePool for address selecting. See Common/HostResolvePool.h for more info.
    // - it minimizes number of `Session::connect()`/`Session::reconnect()` calls
    //   - stores only connected and ready to use sessions
    //   - connection could be reused even when limits are reached
    // - soft limit, warn limit, hard limit
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
    class EndpointConnectionPool :
        public std::enable_shared_from_this<EndpointConnectionPool<Session>>,
        public IExtendedPool
    {
    private:
        friend class DB::ConnectionPools;

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
                LOG_TEST(log, "flushing request IN : {}", response_stream_completed);

                LOG_TEST(log, "~Sessions {}. request_completed {}", Session::getHost(), request_stream_completed && response_stream_completed);

                if (auto lock = pool.lock())
                {
                    lock->atConnectionDestroy(*this);
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
            explicit PooledConnection(EndpointConnectionPool::WeakPtr pool_, DB::ConnectionPoolMetrics metrics_, Args&&... args)
                : Session(args...)
                , pool(std::move(pool_))
                , metrics(std::move(metrics_))
            {
                CurrentMetrics::add(metrics.active_count);
            }

            template<class... Args>
            static Ptr create(Args&&... args)
            {
                struct make_shared_enabler : public PooledConnection
                {
                    explicit make_shared_enabler(Args&&... args) : PooledConnection(std::forward<Args>(args)...) {}
                };
                return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
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
            CurrentMetrics::sub(group->getMetrics().stored_count, stored_connections.size());
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
                auto metrics = getMetrics();
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

        DB::ConnectionPoolMetrics getMetrics() const override
        {
            std::lock_guard lock(mutex);
            return group->getMetrics();
        }

        DB::ConnectionGroupType getGroupType() const override
        {
            std::lock_guard lock(mutex);
            return group->getType();
        }

        void setGroup(ConnectionGroup::Ptr new_group) override
        {
            std::lock_guard lock(mutex);
            group->subConnections(active_connections_in_pool);
            new_group->addConnections(active_connections_in_pool);
            group = new_group;
        }

        size_t wipeExpired() override
        {
            std::vector<ConnectionPtr> expired_connections;

            /// upd stats outsize of critical section
            SCOPE_EXIT({
                auto metrics = getMetrics();
                CurrentMetrics::sub(metrics.stored_count, expired_connections.size());
                ProfileEvents::increment(metrics.expired, expired_connections.size());

                LOG_TEST(log, "expired {} {}", getTarget(), expired_connections.size());
            });

            Poco::Timestamp now;

            std::lock_guard lock(mutex);
            while (!stored_connections.empty())
            {
                auto connection = stored_connections.top();

                if (!isExpired(now, connection))
                    return stored_connections.size();

                stored_connections.pop();
                expired_connections.push_back(connection);
            }

            return stored_connections.size();
        }

    private:
        EndpointConnectionPool(
            ConnectionGroup::Ptr group_,
            String host_,
            UInt16 port_,
            bool https_,
            DB::ProxyConfiguration proxy_configuration_)
        : host(std::move(host_))
        , port(port_)
        , https(https_)
        , proxy_configuration(std::move(proxy_configuration_))
        , resolve_pool(DB::HostResolvePool::create(host, group_->getType()))
        , group(group_)
        {
        }

        WeakPtr getWeakFromThis()
        {
            return EndpointConnectionPool::weak_from_this();
        }

        bool isExpired(Poco::Timestamp & now, ConnectionPtr connection) TSA_REQUIRES(mutex)
        {
            if (group->isSoftLimitReached())
                return now > (connection->getLastRequest() + divide(connection->getKeepAliveTimeout(), 10));
            return now > connection->getLastRequest() + connection->getKeepAliveTimeout();
        }

        ConnectionPtr allocateNewConnection()
        {
            /// Pool is global, we shouldn't attribute this memory to query/user.
            DB::MemoryTrackerSwitcher switcher{&total_memory_tracker};

            ConnectionPtr connection = PooledConnection::create(this->getWeakFromThis(), getMetrics(), host, port);
            connection->setKeepAlive(true);

            if (!proxy_configuration.isEmpty())
                connection->setProxyConfig(proxyConfigurationToPocoProxyConfig(proxy_configuration));

            {
                std::lock_guard lock(mutex);
                ++active_connections_in_pool;
                group->atConnectionCreate();
            }

            return connection;
        }

        ConnectionPtr prepareNewConnection(const DB::ConnectionTimeouts & timeouts)
        {
            auto address = resolve_pool->get();

            auto session = allocateNewConnection();

            setTimeouts(*session, timeouts);
            session->setResolvedHost(*address);

            auto metrics = getMetrics();

            try {
                auto timer = DB::CurrentThread::getProfileEvents().timer(metrics.elapsed_microseconds);
                session->doConnect();
            }
            catch (...)
            {
                address.setFail();
                ProfileEvents::increment(metrics.errors);
                session->reset();
                LOG_TEST(log, "new session failed {}, conn timeout {}ms",
                         getTarget(), timeouts.connection_timeout.totalMilliseconds());
                throw;
            }

            LOG_TEST(log, "new session created {}", getTarget());
            ProfileEvents::increment(metrics.created);
            return session;
        }

        void atConnectionDestroy(PooledConnection & connection)
        {
            size_t preserved = 0;

            /// upd stats outsize of critical section
            SCOPE_EXIT({
                auto metrics = getMetrics();
                CurrentMetrics::add(metrics.stored_count, preserved);
                ProfileEvents::increment(metrics.preserved, preserved);
                ProfileEvents::increment(metrics.reset, 1 - preserved);

                {
                    std::lock_guard lock(mutex);
                    --active_connections_in_pool;
                    group->atConnectionDestroy();
                }
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
                if (group->isHardLimitReached())
                {
                    LOG_TEST(log, "reset hard limit {}", getTarget());
                    return;
                }
            }

            auto connection_to_store = allocateNewConnection();
            connection_to_store->jumpToOtherConnection(connection);

            {
                std::lock_guard lock(mutex);
                stored_connections.push(connection_to_store);
                ++preserved;
            }

            LOG_TEST(log, "preserved {}", getTarget());
        }

        LoggerPtr log = ::getLogger("ConnectionPool");

        const std::string host;
        const UInt16 port;
        const bool https;
        const DB::ProxyConfiguration proxy_configuration;

        DB::HostResolvePool::Ptr resolve_pool;

        mutable std::mutex mutex;
        size_t active_connections_in_pool TSA_GUARDED_BY(mutex) = 0;
        ConnectionsMinHeap stored_connections TSA_GUARDED_BY(mutex);
        ConnectionGroup::Ptr group TSA_GUARDED_BY(mutex);
    };

    struct EndpointPoolKey
    {
        String target_host;
        UInt16 target_port;
        bool is_target_https;
        DB::ProxyConfiguration proxy_config;

        bool operator ==(const EndpointPoolKey & rhs) const
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
    };

    struct Hasher
    {
        size_t operator()(const EndpointPoolKey & k) const
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
    };

    IExtendedPool::Ptr createConnectionPool(
        ConnectionGroup::Ptr group,
        std::string host,
        UInt16 port,
        bool secure,
        DB::ProxyConfiguration proxy_configuration)
    {
        if (secure)
        {
    #if USE_SSL
            return EndpointConnectionPool<Poco::Net::HTTPSClientSession>::create(
                group, std::move(host), port, secure, std::move(proxy_configuration));
    #else
            throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                            "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
    #endif
        }
        else
        {
            return EndpointConnectionPool<Poco::Net::HTTPClientSession>::create(
                group, std::move(host), port, secure, std::move(proxy_configuration));
        }
    }
}

class DB::ConnectionPools::Impl
{
private:
    const size_t DEFAULT_WIPE_TIMEOUT_SECONDS = 5*60;
    const Poco::Timespan wipe_timeout = Poco::Timespan(DEFAULT_WIPE_TIMEOUT_SECONDS, 0);

    ConnectionGroup::Ptr disk_group = std::make_shared<ConnectionGroup>(DB::ConnectionGroupType::DISK);
    ConnectionGroup::Ptr storage_group = std::make_shared<ConnectionGroup>(DB::ConnectionGroupType::STORAGE);
    ConnectionGroup::Ptr http_group = std::make_shared<ConnectionGroup>(DB::ConnectionGroupType::HTTP);

    std::mutex mutex;

    std::unordered_map<EndpointPoolKey, IExtendedPool::Ptr, Hasher> endpoints_pool TSA_GUARDED_BY(mutex);
    Poco::Timestamp last_wipe_time TSA_GUARDED_BY(mutex);

public:
    IEndpointConnectionPool::Ptr getPool(ConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
    {
        Poco::Timestamp now;

        std::lock_guard lock(mutex);

        if (now - last_wipe_time > wipe_timeout)
        {
            wipeExpired();
            last_wipe_time = now;
        }

        auto pool = getPoolImpl(type, uri, proxy_configuration);
        if (type < pool->getGroupType())
        {
            /// pool which is in use of disk could be reached by some url/s3 function or storage
            /// for such pulls applied the group limits with lower value
            pool->setGroup(getGroup(type));
        }

        return pool;
    }

    void setLimits(ConnectionPoolLimits disk, ConnectionPoolLimits storage, ConnectionPoolLimits http)
    {
        disk_group->setLimits(disk);
        storage_group->setLimits(storage);
        http_group->setLimits(http);
    }

    void dropResolvedHostsCache()
    {
        std::vector<IExtendedPool::Ptr> gathered_pools;

        {
            std::lock_guard lock(mutex);

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

    void dropConnectionsCache()
    {
        std::lock_guard lock(mutex);
        endpoints_pool.clear();
    }

protected:
    ConnectionGroup::Ptr & getGroup(ConnectionGroupType type)
    {
        switch (type)
        {
            case ConnectionGroupType::DISK:
                return disk_group;
            case ConnectionGroupType::STORAGE:
                return storage_group;
            case ConnectionGroupType::HTTP:
                return http_group;
        }
    }

    IExtendedPool::Ptr getPoolImpl(
        ConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration) TSA_REQUIRES(mutex)
    {
        auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
        auto key = EndpointPoolKey{host, port, secure, proxy_configuration};

        auto it = endpoints_pool.find(key);
        if (it != endpoints_pool.end())
            return it->second;

        it = endpoints_pool.emplace(
            key,
            createConnectionPool(
                getGroup(type),
                std::move(host),
                port,
                secure,
                proxy_configuration)).first;

        return it->second;
    }

    void wipeExpired() TSA_REQUIRES(mutex)
    {
        std::vector<EndpointPoolKey> keys_to_drop;

        for (auto & [key, pool] : endpoints_pool)
        {
            auto left_connections = pool->wipeExpired();
            if (left_connections == 0 && pool->getGroupType() != ConnectionGroupType::DISK)
                keys_to_drop.push_back(key);
        }

        for (const auto & key : keys_to_drop)
        {
            endpoints_pool.erase(key);
        }
    }

    static bool useSecureConnection(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
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

    static std::tuple<std::string, UInt16, bool> getHostPortSecure(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
    {
        return std::make_tuple(uri.getHost(), uri.getPort(), useSecureConnection(uri, proxy_configuration));
    }
};

DB::ConnectionPools::ConnectionPools()
    : impl(std::make_unique<DB::ConnectionPools::Impl>())
{}

DB::ConnectionPools & DB::ConnectionPools::instance()
{
    static DB::ConnectionPools instance;
    return instance;
}

void DB::ConnectionPools::setLimits(ConnectionPoolLimits disk, ConnectionPoolLimits storage, ConnectionPoolLimits http)
{
    return impl->setLimits(disk, storage, http);
}

void DB::ConnectionPools::dropResolvedHostsCache()
{
    return impl->dropResolvedHostsCache();
}

void DB::ConnectionPools::dropConnectionsCache()
{
    return impl->dropConnectionsCache();
}

DB::IEndpointConnectionPool::Ptr DB::ConnectionPools::getPool(
    DB::ConnectionGroupType type, const Poco::URI & uri, const DB::ProxyConfiguration & proxy_configuration)
{
    return impl->getPool(type, uri, proxy_configuration);
}
