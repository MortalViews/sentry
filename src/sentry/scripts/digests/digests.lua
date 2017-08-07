math.randomseed(seed)

local configuration = {
    ttl = 60 * 60,
}

local function zrange_iterator(result)
    local i = -1
    return function ()
        i = i + 2
        return result[i], result[i+1]
    end
end

local function schedule()
end

local function maintenance()
end

local function add_timeline_to_schedule(timeline, timestamp)
    -- If the timeline is already in the "ready" set, this is a noop.
    if redis.call('ZSCORE', ready, timeline) ~= nil then
        return false
    end

    -- Do scheduling if the timeline is not already in the "ready" set.
    local score = redis.call('ZSCORE', waiting, timeline)
    if score ~= nil then
        -- If the timeline is already in the "waiting" set, increase the delay by
        -- min(current schedule + increment value, maximum delay after last processing time).
        local last_processed = tonumber(redis.call('GET', last_processed_timestamp))
        local update = nil;
        if last_processed == nil then
            -- If the last processed timestamp is missing for some reason (possibly
            -- evicted), be conservative and allow the timeline to be scheduled
            -- with either the current schedule time or provided timestamp,
            -- whichever is smaller.
            update = math.min(score, timestamp)
        else
            update = math.min(
                score + tonumber(increment),
                last + tonumber(maximum)
            )
        end

        if update ~= score then
            redis.call('ZADD', waiting, update, timeline)
        end
        return false
    end

    -- If the timeline isn't already in either set, add it to the "ready" set with
    -- the provided timestamp. This allows for immediate scheduling, bypassing the
    -- imposed delay of the "waiting" state.
    redis.call('ZADD', READY, TIMESTAMP, TIMELINE)
    return true
end

local function truncate_timeline(timeline, limit)
    local n = 0

    -- ZCARD is O(1) while ZREVRANGE is O(log(N)+M) so as long as digests are
    -- generally smaller than the limit (which seems like a safe assumption)
    -- then its cheaper just to check here and exit if there's nothing to do.
    if redis.call('ZCARD', timeline) <= limit then
        return n
    end

    local records = redis.call('ZREVRANGE', timeline, limit, -1)
    for key, score in zrange_iterator(record) do
        redis.call('ZREM', key)
        redis.call('DEL', key)
        n = n + 1
    end
    return n
end

local function add_record_to_timeline(timeline, key, value, timestamp)
    -- Store the record in the database.
    redis.call('SETEX', key, value, configuration.ttl)
    redis.call('EXPIRE', timeline, configuration.ttl)

    redis.call('ZADD', timeline, timetamp, key)

    local ready = add_timeline_to_schedule(timeline, timestamp)

    if math.random() < truncation_chance then
        truncate_timeline(timeline, capacity)
    end

    return ready
end

local function digest_timeline(timeline)
    -- Check to ensure that the timeline is in the correct state.

    if redis.call('EXISTS', digest) == 1 then
        -- If the digest set already exists (possibly because we already tried
        -- to send it and failed for some reason), merge any new data into it.
        redis.call('ZUNIONSTORE', digest, 2, timeline, digest, 'AGGREGATE', 'MAX')
        redis.call('DELETE', timeline)
        redis.call('EXPIRE', digest, configuration.ttl)
    else
        -- Otherwise, we can just move the timeline contents to the digest key.
        redis.call('RENAME', timeline, digest)
        redis.call('EXPIRE', digest, configuration.ttl)
    end

    local records = redis.call('ZREVRANGE', digest, 0, -1, 'WITHSCORES')
    for key, score in zrange_iterator(records) do
    end

    -- Return the records.

    -- TODO: What should we do if there are records in the digest that weren't
    -- at the beginning of this process? This can happen if locks were overrun
    -- or broken. Leaving the extra records in the digest is only an issue if
    -- we are removing the timeline from all schedules (which only happens if
    -- we retrieved a digest without any records.) So, if there are new records
    -- but no records in the timeline, allow the timeline to be added to the
    -- ready set. If the other records end up being sent, they'll get deleted
    -- and the timeline will subsequently get readded to the waiting set
    -- anyway. If the other records end up /not/ being sent, they'll be
    -- attempted to be sent on the next scheduled interval.

    -- If there was data that resulted in a digest being sent: move it back to the ready set.
    -- If there was no data that resulted in a digest being sent,
    -- * If there are no timeline contents: remove it from the ready set (all sets).
    -- * If there are timeline contents: move it to the ready set.
end
