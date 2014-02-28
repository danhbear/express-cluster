Q = require "q"
NodeCache = require "node-cache"

#
# An async wrapper for a shared in-memory cache, maintained by the master.
#

class ClusterCache
  getId: -> Math.random().toString()

  # 
  # Called by cluster master to wire up cache get/set requests from
  # (and responses to) a cluster worker.
  #
  registerWorker: (worker, config = {}) ->
    unless @cache?
      @cache = new NodeCache
        stdTTL: config.cacheTTL ? 60
        checkperiod: config.cacheCheckPeriod ? 30

    worker.on "message", (msg) =>
      if msg.cmd is "cacheGetRequest"
        @cache.get msg.key, (error, value) ->
          console.log "ClusterCache cache get error=#{error} value=#{JSON.stringify value}" if config.verbose
          resp = 
            cmd: "cacheGetResponse"
            id: msg.id
          if error? 
            resp.error = error
          else if value?[msg.key]?
            resp.value = value[msg.key]
          else
            resp.error = "not found or expired"
          worker.send resp
      else if msg.cmd is "cacheSetRequest"
        @cache.set msg.key, msg.value, (error, success) ->
          console.log "ClusterCache cache set error=#{error} success=#{success}" if config.verbose
          resp = 
            cmd: "cacheSetResponse"
            id: msg.id
          if error?
            resp.error = error
          else
            resp.success = success
          worker.send resp

  # 
  # Called by cluster worker to get the cache.
  #
  get: (key) ->
    deferred = Q.defer()
    id = @getId()

    workerListener = (msg) ->
      if msg.cmd is "cacheGetResponse" and msg.id is id
        process.removeListener "message", workerListener
        if msg.error?
          deferred.reject msg.error
        else
          deferred.resolve msg.value
    process.on "message", workerListener

    process.send
      cmd: "cacheGetRequest"
      id: id
      key: key

    deferred.promise

  # 
  # Called by cluster worker to set the cache.
  #
  set: (key, value) ->
    deferred = Q.defer()
    id = @getId()

    workerListener = (msg) ->
      if msg.cmd is "cacheSetResponse" and msg.id is id
        process.removeListener "message", workerListener
        if msg.error?
          deferred.reject msg.error
        else
          deferred.resolve msg.success
    process.on "message", workerListener

    process.send
      cmd: "cacheSetRequest"
      id: id
      key: key
      value: value

    deferred.promise

module.exports = new ClusterCache()
