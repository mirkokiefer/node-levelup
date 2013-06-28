/* Copyright (c) 2012-2013 LevelUP contributors
 * See list at <https://github.com/rvagg/node-levelup#contributing>
 * MIT +no-false-attribs License <https://github.com/rvagg/node-levelup/blob/master/LICENSE>
 */

var Readable       = require('stream').Readable
  , bufferStream = require('simple-bufferstream')
  , inherits     = require('util').inherits
  , extend       = require('xtend')
  , errors       = require('./errors')
  , State        = require('./read-stream-state')

  , toEncoding   = require('./util').toEncoding
  , toSlice      = require('./util').toSlice

  , defaultOptions = { keys: true, values: true }

  , makeKeyValueData = function (key, value) {
      return {
          key: toEncoding[this._keyEncoding](key)
        , value: toEncoding[this._valueEncoding](value)
      }
    }
  , makeKeyData = function (key) {
      return toEncoding[this._keyEncoding](key)
    }
  , makeValueData = function (key, value) {
      return toEncoding[this._valueEncoding](value)
    }
  , makeNoData = function () { return null }

function ReadStream (options, db, iteratorFactory) {
  Readable.call(this, {objectMode: true})

  this._state = State()

  this._dataEvent = 'data'
  this.readable = true
  this.writable = false

  // purely to keep `db` around until we're done so it's not GCed if the user doesn't keep a ref
  this._db = db

  options = this._options = extend(defaultOptions, options)
  this._keyEncoding   = options.keyEncoding   || options.encoding
  this._valueEncoding = options.valueEncoding || options.encoding
  if (typeof this._options.start != 'undefined')
    this._options.start = toSlice[this._keyEncoding](this._options.start)
  if (typeof this._options.end != 'undefined')
    this._options.end = toSlice[this._keyEncoding](this._options.end)
  if (typeof this._options.limit != 'number')
    this._options.limit = -1
  this._options.keyAsBuffer   = this._keyEncoding != 'utf8'   && this._keyEncoding != 'json'
  this._options.valueAsBuffer = this._valueEncoding != 'utf8' && this._valueEncoding != 'json'

  this._makeData = this._options.keys && this._options.values
    ? makeKeyValueData.bind(this) : this._options.keys
      ? makeKeyData.bind(this) : this._options.values
        ? makeValueData.bind(this) : makeNoData


  var ready = function () {
    if (!this._state.canEmitData())
      return

    this._state.ready()
    this._iterator = iteratorFactory(this._options)
    this._read()
  }.bind(this)

  if (db.isOpen())
    ready()
  else
    db.once('ready', ready)
}

inherits(ReadStream, Readable)

ReadStream.prototype._read = function(n) {
  var that = this
  if (!n) return
  this._iterator.next(function(err, key, value) {
    if (err) {
      that.emit('error', err)
    } else if (key === undefined && value === undefined) {
      that._iterator.end(function () {
        that.push(null)
      })
    } else {
      try {
        value = that._makeData(key, value)
        that.push({key: key, value: value})
      } catch (e) {
        return that.emit('error', new errors.EncodingError(e))
      }
    }
  })
}

ReadStream.prototype.toString = function () {
  return 'LevelUP.ReadStream'
}

module.exports.create = function (options, db, iteratorFactory) {
  return new ReadStream(options, db, iteratorFactory)
}
