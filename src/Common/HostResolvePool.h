#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Net/IPAddress.h>

#include <mutex>
#include <memory>

// That class resolves host into multiply addresses
// Features:
// - balance address usage.
//    `selectBest()` chooses the address by random with weights.
//    The more ip is used the lesser weight it has. When new address is happened, it takes more weight.
//    But still not all requests are assigned to the new address.
// - join resolve results
//    In case when host is resolved into different set of addresses, this class join all that addresses and use them.
//    An address expires after `history_` time.
// - failed address pessimization
//    If an address marked with `setFail()` it is marked as faulty. Such address won't be selected until either
//    a) it still occurs in resolve set after `history_` time or b) all other addresses are pessimized as well.
// - resolve schedule
//    Addresses are resolved through `DB::DNSResolver::instance()`.
//    Usually it does not happen more often than once in `history_` time.
//    But also new resolve performed each `setFail()` call.

namespace DB
{

enum class MetricsType
{
    METRICS_FOR_S3_STORAGE,
    METRICS_FOR_S3_DISK,
    METRICS_FOR_HTTP,
};

struct HostResolvePoolMetrics
{
    const ProfileEvents::Event discovered = ProfileEvents::end();
    const ProfileEvents::Event expired = ProfileEvents::end();
    const ProfileEvents::Event failed = ProfileEvents::end();

    const CurrentMetrics::Metric active_count = CurrentMetrics::end();
};

constexpr size_t DEFAULT_RESOLVE_TIME_HISTORY_SECONDS = 2*60;

class HostResolvePool : public std::enable_shared_from_this<HostResolvePool>
{
private:
    using WeakPtr = std::weak_ptr<HostResolvePool>;

public:
    using Ptr = std::shared_ptr<HostResolvePool>;

    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public HostResolvePool
        {
            make_shared_enabler(Args&&... args) : HostResolvePool(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    virtual ~HostResolvePool();

    class Entry
    {
    public:
        explicit Entry(Entry && entry) = default;
        explicit Entry(Entry & entry) = delete;

        // no access as r-value
        const String * operator->() && = delete;
        const String * operator->() const && = delete;
        const String & operator*() && = delete;
        const String & operator*() const && = delete;

        const String * operator->() & { return &resolved_host; }
        const String * operator->() const & { return &resolved_host; }
        const String & operator*() & { return resolved_host; }
        const String & operator*() const & { return resolved_host; }

        void setFail();
        ~Entry();

    private:
        friend class HostResolvePool;

        Entry(HostResolvePool & pool_, Poco::Net::IPAddress address_)
            : pool(pool_.getWeakFromThis())
            , address(std::move(address_))
            , resolved_host(address.toString())
        { }

        HostResolvePool::WeakPtr pool;
        const Poco::Net::IPAddress address;
        const String resolved_host;

        bool fail = false;
    };

    /// can throw NetException(ErrorCodes::DNS_ERROR, ...), Exception(ErrorCodes::BAD_ARGUMENTS, ...)
    Entry get();
    void update();
    void reset();

    static HostResolvePoolMetrics getMetrics(MetricsType type);

protected:
    HostResolvePool(String host_,
                             MetricsType metrics_type,
                             Poco::Timespan history_ = Poco::Timespan(DEFAULT_RESOLVE_TIME_HISTORY_SECONDS, 0));

    using ResolveFunction = std::function<std::vector<Poco::Net::IPAddress> (const String & host)>;
    HostResolvePool(ResolveFunction && resolve_function_,
                    MetricsType metrics_type,
                    String host_,
                    Poco::Timespan history_);

    friend class Entry;
    WeakPtr getWeakFromThis();

    void setSuccess(const Poco::Net::IPAddress & address);
    void setFail(const Poco::Net::IPAddress & address);

    struct Record
    {
        Record(Poco::Net::IPAddress address_, Poco::Timestamp resolve_time_)
            : address(std::move(address_))
            , resolve_time(resolve_time_)
        {}

        explicit Record(Record && rec) = default;
        Record& operator=(Record && s) = default;

        explicit Record(const Record & rec) = default;
        Record& operator=(const Record & s) = default;

        Poco::Net::IPAddress address;
        Poco::Timestamp resolve_time;
        size_t usage = 0;
        bool failed = false;
        Poco::Timestamp fail_time = 0;

        bool operator <(const Record & r) const
        {
            return address < r.address;
        }

        size_t getWeight() const
        {
            /// There is no goal to make usage's distribution ideally even
            /// The goal is to chose more often new address, but still use old addresses as well
            /// when all addresses have usage counter greater than 10000,
            /// no more corrections are needed, just random choice is ok
            if (usage > 10000)
                return 1;
            if (usage > 1000)
                return 5;
            if (usage > 100)
                return 8;
            return 10;
        }
    };

    using Records = std::vector<Record>;

    struct UpdateStats
    {
        size_t added = 0;
        size_t updated = 0;
        size_t expired = 0;
    };

    Poco::Net::IPAddress selectBest() TSA_REQUIRES(mutex);
    Records::iterator find(const Poco::Net::IPAddress & address) TSA_REQUIRES(mutex);
    bool isUpdateNeeded();
    void updateWeights();
    UpdateStats updateImpl(Poco::Timestamp now, std::vector<Poco::Net::IPAddress> & next_gen) TSA_REQUIRES(mutex);
    void initWeightMapImpl() TSA_REQUIRES(mutex);
    void initWeightMap() TSA_REQUIRES(mutex);

    const String host;
    const Poco::Timespan history;
    const HostResolvePoolMetrics metrics;

    // for tests purpose
    const ResolveFunction resolve_function;

    std::mutex mutex;

    Poco::Timestamp last_resolve_time TSA_GUARDED_BY(mutex);
    Records records TSA_GUARDED_BY(mutex);

    size_t total_weight TSA_GUARDED_BY(mutex) = 0;
    std::uniform_int_distribution<size_t> random_weight_picker TSA_GUARDED_BY(mutex);
    struct TWRecord
    {
        size_t weight_prefix_sum = 0;
        size_t record_index = 0;
    };
    using TWeights = std::vector<TWRecord>;
    TWeights weights TSA_GUARDED_BY(mutex);

    Poco::Logger * log = &Poco::Logger::get("ConnectionPool");
};

}

