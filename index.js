const bluebird = require('bluebird')
const redis = require('redis')
const config = require('config')

bluebird.promisifyAll(redis.RedisClient.prototype)
bluebird.promisifyAll(redis.Multi.prototype)

const redisOptions = {
  host: '127.0.0.1',
  port: 6380,
  auth: null,
  ssl: true,
  retry: true,
  prefix: ''
}

try {
  const redisConfig = config.get('redis')
  Object.keys(redisConfig).map((key) => {
    redisOptions[key] = redisConfig[key]
    return null
  })
} catch (e) {
  console.error('Redis config error', e)
}

const opts = {
  password: redisOptions.auth,
  tls: redisOptions.ssl
}
if (redisOptions.retry === true) {
  opts.retry_strategy = (options) => {
    if (options.error && options.error.code === 'ECONNREFUSED') {
      console.log(JSON.stringify({
        app_name: 'exception',
        api_name: 'redis',
        params: JSON.stringify(redisOptions),
        message: 'ECONNREFUSED',
        stack: '',
        dimension: {
          level: 'warning'
        }
      }));
      return new Error('The server refused the connection')
    }
    if (options.total_retry_time > 1000 * 60 * 60) {
      console.log(JSON.stringify({
        app_name: 'exception',
        api_name: 'redis',
        params: JSON.stringify(redisOptions),
        message: 'total retry exhausted',
        stack: '',
        dimension: {
          level: 'critical'
        }
      }));
      return new Error('Retry time exhausted')
    }
    if (options.attempt > 10) {
      console.log(JSON.stringify({
        app_name: 'exception',
        api_name: 'redis',
        params: JSON.stringify(redisOptions),
        message: 'total retry attempts exhausted',
        stack: '',
        dimension: {
          level: 'critical'
        }
      }));
      return 30000 // retry_delay
    }

    return Math.min(options.attempt * 100, 3000)
  }
}

const client = redis.createClient(redisOptions.port || 6379, redisOptions.host, opts)

let error = null
let connectCount = 0

// Redis 错误
client.on('error', (err) => {
  error = err

  console.log(JSON.stringify({
    app_name: 'exception',
    api_name: 'redis',
    params: '{}',
    message: err.message,
    stack: err.stack || '',
    dimension: {
      level: 'critical'
    }    
  }));
})
// Redis 重连
client.on('reconnecting', () => {
  connectCount += 1
  console.log(JSON.stringify({
    app_name: 'exception',
    api_name: 'redis',
    message: 'reconnecting',
    params: '{}',
    stack: '',
    dimension: {
      level: 'warning'
    },
    connectCount
  }));
})
// Redis End
client.on('end', () => {
  console.log(JSON.stringify({
    app_name: 'exception',
    api_name: 'redis',
    message: 'end',
    params: '{}',
    stack: '',
    dimension: {
      level: 'critical'
    }
  }));
})
// Redis Warn
client.on('warning', (err) => {
  console.log(JSON.stringify({
    app_name: 'exception',
    api_name: 'redis',
    message: err.message || err,
    stack: err.stack || '',
    params: '{}',
    dimension: {
      level: 'warning'
    }
  }));
})

const $redis = {
  async get(key) {
    let rs = null

    try {
      rs = await client.getAsync(`${redisOptions.prefix}${key}`)
      if (rs) {
        rs = JSON.parse(rs)
      } else {
        rs = null
      }
    } catch (e) {
      console.log(JSON.stringify({
        app_name: 'exception',
        api_name: 'redis',
        params: JSON.stringify({ key }),
        message: e.message,
        stack: e.stack || '',
        dimension: {
          level: 'info'
        }
      }));
    }

    return rs
  },
  set(key, val, exp = null) {
    let rs = null

    if (exp === null) {
      rs = client.set(`${redisOptions.prefix}${key}`, JSON.stringify(val))
    } else {
      rs = client.set(`${redisOptions.prefix}${key}`, JSON.stringify(val), 'EX', exp)
    }

    return rs
  },
  delete(key) {
    return new Promise((resolve) => {
      try {
        client.del(`${redisOptions.prefix}${key}`)
      } catch (e) {
        console.log(JSON.stringify({
          app_name: 'exception',
          api_name: 'redis',
          params: JSON.stringify({ key }),
          message: e.message,
          stack: e.stack || '',
          dimension: {
            level: 'info'
          }
        }));
      }

      resolve()
    })
  },
  quit() {
    return client.quit()
  },
  end(flush = true) {
    return client.end(flush)
  },
  watch(key) {
    return client.watch(`${redisOptions.prefix}${key}`)
  },
  multi() {
    return client.multi()
  },
  inc(key) {
    return client.incr(`${redisOptions.prefix}${key}`)
  },
  dec(key) {
    return client.decr(`${redisOptions.prefix}${key}`)
  },
  async ping(callback) {
    if (error) {
      callback(error)
    } else {
      await client.ping(async (err, pong) => {
        console.log(err, pong)
        await callback(err, pong)
      })
    }
  },
  getClient() {
    return client
  }
}

module.exports = $redis
