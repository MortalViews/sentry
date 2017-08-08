math.randomseed(seed)

function table.extend(t, items)
    for _, item in ipairs(items) do
        table.insert(t, item)
    end
end

local function zrange_scored_iterator(result)
    local i = -1
    return function ()
        i = i + 2
        return result[i], result[i+1]
    end
end

local function schedule(deadline)
    -- TODO: Maybe switch this over to ZSCAN to allow iterative processing?
    local timelines = redis.call('ZRANGEBYSCORE', 0, deadline, 'WITHSCORES')

    local remove = {}
    local update = {}
    local results = {}
    for key, timestamp in zrange_scored_iterator(timelines) do
        table.insert(remove, key)
        table.extend(update, {timestamp, key})
        table.extend(results, {key, timestamp})
    end

    redis.call('ZREM', waiting, unpack(remove))
    redis.call('ZADD', ready, unpack(update))

    return results
end

local function maintenance(deadline)
    error('not implemented')
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
    for _, key in ipairs(records) do
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
    for key, score in zrange_scored_iterator(records) do
        error('return records')
    end

    -- Return the records to the client.
end

local function close_digest(timeline, records)
    -- Remove the records from the digest.
    redis.call('ZREM', digest, unpack(records))
    for _, record in ipairs(records) do
        redis.call('DEL', record)
    end

    -- We always add to the ready set if we digested any number of records or
    -- there are contents waiting to be delivered.
    if #records > 0 or redis.call('ZCARD', timeline) > 0 or redis.call('ZCARD', digest) > 0 then
        redis.call('SETEX', last_processed, configuration.ttl, value)
        redis.call('ZREM', ready, timeline)
        redis.call('ZADD', waiting, timestamp + minimum_delay, timeline)
    else
        redis.call('DEL', last_processed)
        redis.call('ZREM', ready, timeline)
        redis.call('ZREM', waiting, timeline)
    end
end

local function delete_timeline(configuration, timeline_id)
    truncate_timeline(timeline, 0)
    truncate_timeline(digest, 0)
    redis.call('DEL', configuration:get_timeline_last_processed_timestamp_key(timeline_id))
    redis.call('ZREM', configuration:get_schedule_ready_key(), timeline_id)
    redis.call('ZREM', configuration:get_schedule_waiting_key(), timeline_id)
end

local function parse_arguments(arguments)
    -- TODO: fill in
    local configuration = {
        namespace = 'd',
        ttl = 60 * 60,
    }

    function configuration:get_schedule_waiting_key()
        return string.format('%s:s:w', self.namespace)
    end

    function configuration:get_schedule_ready_key()
        return string.format('%s:s:r', self.namespace)
    end

    function configuration:get_timeline_key(timeline_id)
        return string.format('%s:t:%s', self.namespace, timeline_id)
    end

    function configuration:get_timeline_digest_key(timeline_id)
        return string.format('%s:t:%s:d', self.namespace, timeline_id)
    end

    function configuration:get_timeline_last_processed_timestamp_key(timeline_id)
        return string.format('%s:t:%s:l', self.namespace, timeline_id)
    end

    function configuration:get_timeline_record_key(timeline_id, record_id)
        return string.format('%s:t:%s:r:%s', self.namespace, timeline_id, record_id)
    end

    return configuration, arguments
end

local commands = {
    SCHEDULE = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end,
    MAINTENANCE = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end
    ADD = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end,
    DELETE = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end,
    DIGEST_OPEN = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end,
    DIGEST_CLOSE = function (keys, arguments)
        local configuration, arguments = parse_arguments(arguments)
        error('not implemented')
    end,
}
