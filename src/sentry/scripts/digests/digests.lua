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

local function add_record_to_timeline(timeline, key, value, timestamp)
    redis.call('SETEX', key, value, configuration.ttl)
    redis.call('ZADD', timeline, timetamp, key)
    redis.call('EXPIRE', timeline, configuration.ttl)

    -- Do scheduling.

    -- Truncate if necessary.
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
