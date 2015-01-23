class Huabot
  constructor: (@host) ->

  _request: (uri, options, callback) ->
    if typeof options is 'function'
      callback = options
      options = {}

    if typeof uri is 'object'
      options = uri
    else
      options.uri = uri

    jQuery.ajax {
      url: options.uri
      data: options.data
      dataType: 'json'
      method: options.method
      timeout: options.timeout
      success: (data, status, xhr) ->
        return callback data.err if data.err
        callback null, data
      error: (xhr, status, err) ->
        callback err
      # complete: (xhr, status) ->
    }

  auth: (username, passwd, callback) ->
    @_request '/api/auth', {
      data: {
        username: username,
        passwd: passwd
      }
      method: 'POST'
    }, callback

  unauth: (callback) ->
    @_request '/api/unauth', callback

  saveRobot: (data, callback) ->
    keys = ['name', 'passwd', 'subscribe', 'day_limit', 'one_by_one']
    requiredKeys = ['name', 'passwd']
    formData = {}
    extraData = {}

    robotId = null
    if data.robot_id
      robotId = data.robot_id
      delete data.robot_id

    for k, v of data
      return callback "#{k} is required" if k in requiredKeys and !v
      if k in keys
        formData[k] = v
      else
        extraData[k] = v

    formData['extra'] = JSON.stringify extraData

    uri = '/api/robots/'
    uri = "#{uri}#{robotId}" if robotId
    @_request uri, {
      data: formData,
      method: 'POST'
    }, callback

  removeRobot: (robotId, callback) ->
    @_request "/api/robots/#{robotId}", {method: 'DELETE'}, callback

  startRobot: (robotId, callback) ->
    @_request "/api/robots/#{robotId}/start", {method: 'POST'}, callback

  stopRobot: (robotId, callback) ->
    @_request "/api/robots/#{robotId}/stop", {method: 'POST'}, callback

  getRobots: (page, limit, callback) ->
    @_request "/api/robots?page=#{}&limit=#{limit}", callback

  saveTask: (data, callback) ->
    keys = ['url', 'desc', 'spider', 'proxies', 'refresh_delay']
    requiredKeys = ['url', 'spider']
    formData = {}
    extraData = {}

    taskId = null
    if data.task_id
      taskId = data.task_id
      delete data.task_id

    for k, v of data
      return callback "#{k} is required" if k in requiredKeys and !v
      if k in keys
        formData[k] = v
      else
        extraData[k] = v

    formData['extra'] = JSON.stringify extraData

    uri = '/api/tasks/'
    uri = "#{uri}#{taskId}" if taskId
    @_request uri, {
      data: formData,
      method: 'POST'
    }, callback

  removeTask: (taskId, callback) ->
    @_request "/api/tasks/#{taskId}", {method: 'DELETE'}, callback

  removeTaskLink: (taskId, callback) ->
    @_request "/api/tasks/#{taskId}/clear_uniq", {method: 'POST'}, callback

  getTasks: (page, limit, callback) ->
    @_request "/api/tasks?page=#{}&limit=#{limit}", callback

  getSuccedCount: (dateType, callback) ->
    @_request "/api/#{dateType}/succeed_count", callback

  getRobotSuccedCount: (robotId, dateType, callback) ->
    @_request "/api/robots/#{robotId}/#{dateType}/succeed_count", callback

  getTaskSuccedCount: (taskId, dateType, callback) ->
    @_request "/api/tasks/#{taskId}/#{dateType}/succeed_count", callback
