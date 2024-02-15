#include <Common/HostResolvePool.h>

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/ErrorCodes.h>
#include <Common/thread_local_rng.h>

#include <mutex>

namespace ProfileEvents
{
    extern const Event S3StorageAddressesDiscovered;
    extern const Event S3StorageAddressesExpired;
    extern const Event S3StorageAddressesFailScored;

    extern const Event S3DiskAddressesDiscovered;
    extern const Event S3DiskAddressesExpired;
    extern const Event S3DiskAddressesFailScored;

    extern const Event HttpAddressesDiscovered;
    extern const Event HttpAddressesExpired;
    extern const Event HttpAddressesFailScored;
}

namespace CurrentMetrics
{
    extern const Metric S3StorageAddressesActive;
    extern const Metric S3DiskAddressesActive;
    extern const Metric HttpAddressesActive;
}

namespace DB::ErrorCodes
{
    extern const int DNS_ERROR;
}

DB::HostResolvePoolMetrics getMetricsForS3StorageHostResolve()
{
    return DB::HostResolvePoolMetrics {
        .discovered = ProfileEvents::S3StorageAddressesDiscovered,
        .expired = ProfileEvents::S3StorageAddressesExpired,
        .failed = ProfileEvents::S3StorageAddressesFailScored,
        .active_count = CurrentMetrics::S3StorageAddressesActive,
    };
}

DB::HostResolvePoolMetrics getMetricsForS3DiskHostResolve()
{
    return DB::HostResolvePoolMetrics {
        .discovered = ProfileEvents::S3DiskAddressesDiscovered,
        .expired = ProfileEvents::S3DiskAddressesExpired,
        .failed = ProfileEvents::S3DiskAddressesFailScored,
        .active_count = CurrentMetrics::S3DiskAddressesActive,
    };
}

DB::HostResolvePoolMetrics getMetricsForHttpHostResolve()
{
    return DB::HostResolvePoolMetrics {
        .discovered = ProfileEvents::HttpAddressesDiscovered,
        .expired = ProfileEvents::HttpAddressesExpired,
        .failed = ProfileEvents::HttpAddressesFailScored,
        .active_count = CurrentMetrics::HttpAddressesActive,
    };
}

DB::HostResolvePoolMetrics DB::HostResolvePool::getMetrics(MetricsType type)
{
    switch (type)
    {
        case MetricsType::METRICS_FOR_S3_STORAGE:
            return getMetricsForS3StorageHostResolve();
        case MetricsType::METRICS_FOR_S3_DISK:
            return getMetricsForS3DiskHostResolve();
        case MetricsType::METRICS_FOR_HTTP:
            return getMetricsForHttpHostResolve();
    }
}

DB::HostResolvePool::WeakPtr DB::HostResolvePool::getWeakFromThis()
{
    return weak_from_this();
}

DB::HostResolvePool::HostResolvePool(
    String host_,
    MetricsType metrics_type,
    Poco::Timespan history_)
    : host(std::move(host_))
    , history(history_)
    , metrics(getMetrics(metrics_type))
    , resolve_function([] (const String & host_to_resolve)
    {
      return DB::DNSResolver::instance().resolveHostAll(host_to_resolve);
    })
{
    update();
}

DB::HostResolvePool::HostResolvePool(
    ResolveFunction && resolve_function_,
    MetricsType metrics_type,
    String host_,
    Poco::Timespan history_)
    : host(std::move(host_))
    , history(history_)
    , metrics(getMetrics(metrics_type))
    , resolve_function(std::move(resolve_function_))
{
    update();
}

DB::HostResolvePool::~HostResolvePool()
{
    std::lock_guard lock(mutex);
    CurrentMetrics::sub(metrics.active_count, records.size());
    records.clear();
}

void DB::HostResolvePool::Entry::setFail()
{
    fail = true;

    if (auto lock = pool.lock())
    {
        lock->setFail(address);
    }
}

DB::HostResolvePool::Entry::~Entry()
{
    if (!fail)
    {
        if (auto lock = pool.lock())
        {
            lock->setSuccess(address);
        }
    }
}

void DB::HostResolvePool::update()
{
    auto next_gen = resolve_function(host);
    if (next_gen.empty())
        throw DB::NetException(ErrorCodes::DNS_ERROR, "no endpoints resolved for host {}", host);

    std::sort(next_gen.begin(), next_gen.end());

    UpdateStats stats;

    /// upd stats outsize of critical section
    SCOPE_EXIT({
        CurrentMetrics::add(metrics.active_count, stats.added);
        CurrentMetrics::sub(metrics.active_count, stats.expired);
        ProfileEvents::increment(metrics.discovered, stats.added);
        ProfileEvents::increment(metrics.expired, stats.expired);
    });

    Poco::Timestamp now;

    std::lock_guard lock(mutex);
    stats = updateImpl(now, next_gen);
}

void DB::HostResolvePool::reset()
{
    std::lock_guard lock(mutex);

    CurrentMetrics::sub(metrics.active_count, records.size());
    records.clear();
    weights.clear();
}

void DB::HostResolvePool::updateWeights()
{
    std::lock_guard lock(mutex);
    initWeightMap();
}

DB::HostResolvePool::Entry DB::HostResolvePool::get()
{
    if (isUpdateNeeded())
        update();

    std::lock_guard lock(mutex);
    return Entry(*this, selectBest());
}

void DB::HostResolvePool::setSuccess(const Poco::Net::IPAddress & address)
{
    size_t old_weight = 0;
    size_t new_weight = 0;

    SCOPE_EXIT({
        if (old_weight != new_weight)
        {
            updateWeights();
        }
    });

    std::lock_guard lock(mutex);

    auto it = find(address);
    if (it == records.end())
        return;

    old_weight = it->getWeight();
    ++it->usage;
    new_weight = it->getWeight();
}

void DB::HostResolvePool::setFail(const Poco::Net::IPAddress & address)
{
    Poco::Timestamp now;

    {
        std::lock_guard lock(mutex);

        auto it = find(address);
        if (it == records.end())
            return;

        it->failed = true;
        it->fail_time = now;
    }

    ProfileEvents::increment(metrics.failed);
    update();
}

Poco::Net::IPAddress DB::HostResolvePool::selectBest()
{
    chassert(!records.empty());
    size_t weight = random_weight_picker(thread_local_rng);
    auto it = std::lower_bound(
        weights.begin(), weights.end(),
        weight,
        [] (const TWRecord & rec, size_t value)
        {
            return rec.weight_prefix_sum < value;
        });
    chassert(it != weights.end());
    chassert(it->record_index < records.size());
    return records[it->record_index].address;
}

DB::HostResolvePool::Records::iterator DB::HostResolvePool::find(const Poco::Net::IPAddress & addr) TSA_REQUIRES(mutex)
{
    return std::lower_bound(
        records.begin(), records.end(),
        addr,
        [] (const Record& rec, const Poco::Net::IPAddress & value)
        {
            return rec.address < value;
        });
}

bool DB::HostResolvePool::isUpdateNeeded()
{
        Poco::Timestamp now;

        std::lock_guard lock(mutex);
        return last_resolve_time + history < now || records.empty();
}

DB::HostResolvePool::UpdateStats DB::HostResolvePool::updateImpl(Poco::Timestamp now, std::vector<Poco::Net::IPAddress> & next_gen) TSA_REQUIRES(mutex)
{
    UpdateStats stats;

    const auto last_effective_resolve = now - history;

    Records merged;
    merged.reserve(records.size() + next_gen.size());

    auto it_before = records.begin();
    auto it_next = next_gen.begin();
    while (it_before != records.end() && it_next != next_gen.end())
    {
        if (it_before->address == *it_next)
        {
            ++stats.updated;

            merged.push_back(*it_before);
            merged.back().resolve_time = now;

            ++it_before;
            ++it_next;
        }
        else if (it_before->address < *it_next)
        {
            if (it_before->resolve_time >= last_effective_resolve)
            {
                merged.push_back(*it_before);
            }
            else
            {
                ++stats.expired;
            }
            ++it_before;
        }
        else
        {
            ++stats.added;
            merged.push_back(Record(*it_next, now));
            ++it_next;
        }
    }

    while (it_before != records.end())
    {
        if (it_before->resolve_time >= last_effective_resolve)
        {
            merged.push_back(*it_before);
        }
        else
        {
            ++stats.expired;
        }
        ++it_before;
    }

    while (it_next != next_gen.end())
    {
        ++stats.added;
        merged.push_back(Record(*it_next, now));
        ++it_next;
    }

    for (auto & rec : merged)
    {
        if (rec.failed && rec.fail_time < last_effective_resolve)
            rec.failed = false;
    }

    chassert(std::is_sorted(merged.begin(), merged.end()));

    last_resolve_time = now;
    records.swap(merged);

    if (records.empty())
        throw DB::NetException(ErrorCodes::DNS_ERROR, "no endpoints resolved for host {}", host);

    initWeightMap();

    return stats;
}

void DB::HostResolvePool::initWeightMapImpl()
{
    size_t total_weight_next = 0;

    TWeights weights_next;
    weights_next.reserve(records.size());

    for (size_t i = 0; i < records.size(); ++i)
    {
        auto & rec = records[i];
        if (rec.failed)
            continue;

        total_weight_next += rec.getWeight();
        weights_next.push_back(
            TWRecord{
                .weight_prefix_sum = total_weight_next,
                .record_index = i});
    }

    chassert(weights_next.size() <= records.size());

    total_weight = total_weight_next;
    weights.swap(weights_next);
}

void DB::HostResolvePool::initWeightMap()
{
    initWeightMapImpl();

    if (total_weight == 0 && !records.empty())
    {
        for (auto & rec: records)
        {
            rec.failed = false;
        }

        initWeightMapImpl();
    }

    chassert((total_weight > 0 && !weights.empty()) || records.empty());
    random_weight_picker = std::uniform_int_distribution<size_t>(0, total_weight);
}
