/*
 * Includes code from:
 *
 * base64-arraybuffer
 * https://github.com/niklasvh/base64-arraybuffer
 *
 * Copyright (c) 2012 Niklas von Hertzen
 * Licensed under the MIT license.
 */
(function() {
    'use strict';

    // Promises!
    var Promise = (typeof module !== 'undefined' && module.exports) ?
                  require('promise') : this.Promise;

    var openDatabase = this.openDatabase;


    // If WebSQL methods aren't available, we can stop now.
    if (!openDatabase) {
        return;
    }


    var _CHUNKSIZE_KB = 256;

    // Sadly, the best way to save binary data in WebSQL is Base64 serializing
    // it, so this is how we store it to prevent very strange errors with less
    // verbose ways of binary <-> string data storage.
    var BASE_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

    var SERIALIZED_MARKER = '__lfsc__:';
    var SERIALIZED_MARKER_LENGTH = SERIALIZED_MARKER.length;

    // OMG the serializations!
    var TYPE_ARRAYBUFFER = 'arbf';
    var TYPE_BLOB = 'blob';
    var TYPE_INT8ARRAY = 'si08';
    var TYPE_UINT8ARRAY = 'ui08';
    var TYPE_UINT8CLAMPEDARRAY = 'uic8';
    var TYPE_INT16ARRAY = 'si16';
    var TYPE_INT32ARRAY = 'si32';
    var TYPE_UINT16ARRAY = 'ur16';
    var TYPE_UINT32ARRAY = 'ui32';
    var TYPE_FLOAT32ARRAY = 'fl32';
    var TYPE_FLOAT64ARRAY = 'fl64';
    var TYPE_SERIALIZED_MARKER_LENGTH = SERIALIZED_MARKER_LENGTH +
                                        TYPE_ARRAYBUFFER.length;
    // Converts a buffer to a string to store, serialized, in the backend
    // storage library.
    function _bufferToString(buffer) {
        // base64-arraybuffer
        var bytes = new Uint8Array(buffer);
        var i;
        var base64String = '';

        for (i = 0; i < bytes.length; i += 3) {
            /*jslint bitwise: true */
            base64String += BASE_CHARS[bytes[i] >> 2];
            base64String += BASE_CHARS[((bytes[i] & 3) << 4) | (bytes[i + 1] >> 4)];
            base64String += BASE_CHARS[((bytes[i + 1] & 15) << 2) | (bytes[i + 2] >> 6)];
            base64String += BASE_CHARS[bytes[i + 2] & 63];
        }

        if ((bytes.length % 3) === 2) {
            base64String = base64String.substring(0, base64String.length - 1) + '=';
        } else if (bytes.length % 3 === 1) {
            base64String = base64String.substring(0, base64String.length - 2) + '==';
        }

        return base64String;
    }

    // Deserialize data we've inserted into a value column/field. We place
    // special markers into our strings to mark them as encoded; this isn't
    // as nice as a meta field, but it's the only sane thing we can do whilst
    // keeping localStorage support intact.
    //
    // Oftentimes this will just deserialize JSON content, but if we have a
    // special marker (SERIALIZED_MARKER, defined above), we will extract
    // some kind of arraybuffer/binary data/typed array out of the string.
    function _deserialize(value) {
        // If we haven't marked this string as being specially serialized (i.e.
        // something other than serialized JSON), we can just return it and be
        // done with it.
        if (value.substring(0,
                            SERIALIZED_MARKER_LENGTH) !== SERIALIZED_MARKER) {
            return JSON.parse(value);
        }

        // The following code deals with deserializing some kind of Blob or
        // TypedArray. First we separate out the type of data we're dealing
        // with from the data itself.
        var serializedString = value.substring(TYPE_SERIALIZED_MARKER_LENGTH);
        var type = value.substring(SERIALIZED_MARKER_LENGTH,
                                   TYPE_SERIALIZED_MARKER_LENGTH);

        // Fill the string into a ArrayBuffer.
        var bufferLength = serializedString.length * 0.75;
        var len = serializedString.length;
        var i;
        var p = 0;
        var encoded1, encoded2, encoded3, encoded4;

        if (serializedString[serializedString.length - 1] === '=') {
            bufferLength--;
            if (serializedString[serializedString.length - 2] === '=') {
                bufferLength--;
            }
        }

        var buffer = new ArrayBuffer(bufferLength);
        var bytes = new Uint8Array(buffer);

        for (i = 0; i < len; i+=4) {
            encoded1 = BASE_CHARS.indexOf(serializedString[i]);
            encoded2 = BASE_CHARS.indexOf(serializedString[i+1]);
            encoded3 = BASE_CHARS.indexOf(serializedString[i+2]);
            encoded4 = BASE_CHARS.indexOf(serializedString[i+3]);

            /*jslint bitwise: true */
            bytes[p++] = (encoded1 << 2) | (encoded2 >> 4);
            bytes[p++] = ((encoded2 & 15) << 4) | (encoded3 >> 2);
            bytes[p++] = ((encoded3 & 3) << 6) | (encoded4 & 63);
        }

        // Return the right type based on the code/type set during
        // serialization.
        switch (type) {
            case TYPE_ARRAYBUFFER:
                return buffer;
            case TYPE_BLOB:
                return new Blob([buffer]);
            case TYPE_INT8ARRAY:
                return new Int8Array(buffer);
            case TYPE_UINT8ARRAY:
                return new Uint8Array(buffer);
            case TYPE_UINT8CLAMPEDARRAY:
                return new Uint8ClampedArray(buffer);
            case TYPE_INT16ARRAY:
                return new Int16Array(buffer);
            case TYPE_UINT16ARRAY:
                return new Uint16Array(buffer);
            case TYPE_INT32ARRAY:
                return new Int32Array(buffer);
            case TYPE_UINT32ARRAY:
                return new Uint32Array(buffer);
            case TYPE_FLOAT32ARRAY:
                return new Float32Array(buffer);
            case TYPE_FLOAT64ARRAY:
                return new Float64Array(buffer);
            default:
                throw new Error('Unkown type: ' + type);
        }
    }

    // Serialize a value, afterwards executing a callback (which usually
    // instructs the `setItem()` callback/promise to be executed). This is how
    // we store binary data with localStorage.
    function _serialize(value) {
        return new Promise(function(resolve, reject) {
            var valueString = '';
            if (value) {
                valueString = value.toString();
            }

            // Cannot use `value instanceof ArrayBuffer` or such here, as these
            // checks fail when running the tests using casper.js...
            //
            // TODO: See why those tests fail and use a better solution.
            if (value && (value.toString() === '[object ArrayBuffer]' ||
                          value.buffer &&
                          value.buffer.toString() === '[object ArrayBuffer]')) {
                // Convert binary arrays to a string and prefix the string with
                // a special marker.
                var buffer;
                var marker = SERIALIZED_MARKER;

                if (value instanceof ArrayBuffer) {
                    buffer = value;
                    marker += TYPE_ARRAYBUFFER;
                } else {
                    buffer = value.buffer;

                    if (valueString === '[object Int8Array]') {
                        marker += TYPE_INT8ARRAY;
                    } else if (valueString === '[object Uint8Array]') {
                        marker += TYPE_UINT8ARRAY;
                    } else if (valueString === '[object Uint8ClampedArray]') {
                        marker += TYPE_UINT8CLAMPEDARRAY;
                    } else if (valueString === '[object Int16Array]') {
                        marker += TYPE_INT16ARRAY;
                    } else if (valueString === '[object Uint16Array]') {
                        marker += TYPE_UINT16ARRAY;
                    } else if (valueString === '[object Int32Array]') {
                        marker += TYPE_INT32ARRAY;
                    } else if (valueString === '[object Uint32Array]') {
                        marker += TYPE_UINT32ARRAY;
                    } else if (valueString === '[object Float32Array]') {
                        marker += TYPE_FLOAT32ARRAY;
                    } else if (valueString === '[object Float64Array]') {
                        marker += TYPE_FLOAT64ARRAY;
                    } else {
                        reject(new Error('Failed to get type for BinaryArray'));
                    }
                }

                resolve(marker + _bufferToString(buffer));
            } else if (valueString === '[object Blob]') {
                // Conver the blob to a binaryArray and then to a string.
                var fileReader = new FileReader();

                fileReader.onload = function() {
                    var str = _bufferToString(this.result);

                    resolve(SERIALIZED_MARKER + TYPE_BLOB + str);
                };

                fileReader.readAsArrayBuffer(value);
            } else {
                try {
                    resolve(JSON.stringify(value));
                } catch (e) {
                    window.console.error("Couldn't convert value into a JSON " +
                                         'string: ', value);

                    reject(null, e);
                }
            }
        });
    }



    function iterate(iterator, callback) {
        var self = this;

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;

                dbInfo.db.transaction(function(t) {
                    t.executeSql('SELECT * FROM ' + dbInfo.storeName, [],
                        function(t, results) {
                            var rows = results.rows;
                            var length = rows.length;

                            for (var i = 0; i < length; i++) {
                                var item = rows.item(i);
                                var result = item.value;

                                // Check to see if this is serialized content
                                // we need to unpack.

                                result = iterator(result, item.key);

                                // void(0) prevents problems with redefinition
                                // of `undefined`.
                                if (result !== void(0)) {
                                    resolve(result);
                                    return;
                                }
                            }

                            resolve();
                        }, function(t, error) {
                            reject(error);
                        });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }




    function _initStorage(options) {
        var self = this;
        var dbInfo = {
            db: null
        };

        if (options) {
            for (var i in options) {
                dbInfo[i] = typeof(options[i]) !== 'string' ?
                            options[i].toString() : options[i];
            }
        }

        return new Promise(function(resolve, reject) {
            // Open the database; the openDatabase API will automatically
            // create it for us if it doesn't exist.
            try {
                dbInfo.db = openDatabase(dbInfo.name, String(dbInfo.version),
                                         dbInfo.description, dbInfo.size);
            } catch (e) {
                return self.setDriver('localStorageWrapper')
                    .then(function() {
                        return self._initStorage(options);
                    })
                    .then(resolve)
                    .catch(reject);
            }

            // Create our key/value table if it doesn't exist.
            dbInfo.db.transaction(function(t) {
                t.executeSql('CREATE TABLE IF NOT EXISTS ' + dbInfo.storeName +
                             ' (id NVARCHAR(256), data TEXT, part INT, timestamp REAL)', [],
                             function() {
                    self._dbInfo = dbInfo;
                    resolve();
                }, function(t, error) {
                    console.log(error);
                    reject(error);
                });
            });
        });

    }


    function clear(callback) {
        var self = this;

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {
                    t.executeSql('DELETE FROM ' + dbInfo.storeName, [],
                                 function() {
                        resolve();
                    }, function(t, error) {
                        reject(error);
                    });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }



    function getItem(key, callback) {
        var self = this;

        // Cast the key to a string, as that's all we can set as a key.
        if (typeof key !== 'string') {
            window.console.warn(key +
                                ' used as a key, but it is not a string.');
            key = String(key);
        }
        
        var promise = new Promise(function(resolve, reject) {

            var getItemInternal = function(t, result) {
                var noOfRows = result.rows.length;

                // No results available
                if (noOfRows === 0) {
                  reject("empty response");
                  return;
                }

                var dataInternal = '';

                for (var i = 0, ii = noOfRows; i < ii; i++) {
                    dataInternal += JSON.parse(result.rows.item(i).data);
                }

                resolve(_deserialize(dataInternal));
            };


        
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {

                    t.executeSql('SELECT id, data FROM ' + dbInfo.storeName +
                                 ' WHERE id = ? ORDER BY part ASC',
                                 [key], getItemInternal,
                                 function(t, error) {
                                    reject(error);
                                 });
                });
            }).catch(reject);
        });




        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }

        return promise;
    }


    function key(n, callback) {
        var self = this;

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {
                    t.executeSql('SELECT id FROM ' + dbInfo.storeName +
                                 ' WHERE id = ? LIMIT 1', [n + 1],
                                 function(t, results) {
                        var result = results.rows.length ?
                                     results.rows.item(0).key : null;
                        resolve(result);
                    }, function(t, error) {
                        reject(error);
                    });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }

    function keys(callback) {
        var self = this;

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {
                    t.executeSql('SELECT id FROM ' + dbInfo.storeName, [],
                                 function(t, results) {
                        var keys = [];

                        for (var i = 0; i < results.rows.length; i++) {
                            keys.push(results.rows.item(i).key);
                        }

                        resolve(keys);
                    }, function(t, error) {

                        reject(error);
                    });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }

    function length(callback) {
        var self = this;

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {
                    // Ahhh, SQL makes this one soooooo easy.
                    t.executeSql('SELECT COUNT(id) as c FROM ' +
                                 dbInfo.storeName, [], function(t, results) {
                        var result = results.rows.item(0).c;

                        resolve(result);
                    }, function(t, error) {

                        reject(error);
                    });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }

    function removeItem(key, callback) {
        var self = this;

        // Cast the key to a string, as that's all we can set as a key.
        if (typeof key !== 'string') {
            window.console.warn(key +
                                ' used as a key, but it is not a string.');
            key = String(key);
        }

        var promise = new Promise(function(resolve, reject) {
            self.ready().then(function() {
                var dbInfo = self._dbInfo;
                dbInfo.db.transaction(function(t) {
                    t.executeSql('DELETE FROM ' + dbInfo.storeName +
                                 ' WHERE id = ?', [key], function() {

                        resolve();
                    }, function(t, error) {

                        reject(error);
                    });
                });
            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;
    }

    function _storeSegment(id, data, partNo, dbInfo) {
        var promise = new Promise(function(resolve, reject) {

            var sql = 'INSERT OR REPLACE INTO ' + dbInfo.storeName + ' (data, part, timestamp, id) VALUES (?, ?, ?, ?)';
            var storeSegmentInternal = function() {

                dbInfo.db.transaction(function(t) {
                    var timestamp = Date.now();
                    var insertData = [ JSON.stringify(data), partNo, timestamp, id ];
                    t.executeSql(sql, insertData, function() {
                        resolve();
                    }, function(t, error) {
                        reject(error);
                    });

                }, function(error){
                    reject(error);
                });
            };

            //lets check if there is already a stored id
            if (partNo <= 0) {
                dbInfo.db.transaction(function(t) {
                    //start exists check
                    t.executeSql('SELECT id FROM ' +
                        dbInfo.storeName +
                        ' WHERE id = ?', [id],
                        function(t, result) {
                            //if result > 0 delete them
                            if(Boolean(result.rows.length)){
                                dbInfo.db.transaction(function(t) {
                                    t.executeSql('DELETE FROM ' +
                                    dbInfo.storeName +
                                    ' WHERE id = ?', [id],
                                    function(){
                                        storeSegmentInternal();
                                    },
                                    function(t, error){
                                        reject(error);
                                    });
                                });
                            } else {
                                //result <= 0, store them
                                storeSegmentInternal();
                            }
                        }, function(t,error) {
                            reject(error);
                        });
                });

            } else {
                storeSegmentInternal();
            }

        });

      return promise;
    }


    function setItem(id, data, callback) {
        var self = this;

        // Cast the key to a string, as that's all we can set as a key.
        if (typeof id !== 'string') {
            window.console.warn(id +
                                ' used as a key, but it is not a string.');
            id = String(id);
        }

        var promise = new Promise(function(resolve, reject) {

            self.ready().then(function() {

                _serialize(data).then(function(value){

                    var length = value.length;
                    var maxChunkSize = _CHUNKSIZE_KB * 1024;
                    if (length <= maxChunkSize) {

                        _storeSegment(id, value, -1, self._dbInfo)
                        .then(function(){
                            resolve();
                        },function(error){
                            reject(error);
                        });
                    } else {
                        // Segment byte data into a set of string chunks
                        var startPos = 0;
                        var partNo = 0;


                        var saveInternal = function() {
                            var chunk;
                            if (startPos + maxChunkSize <= length) {
                                // Remaining data does not fit into one chunk
                                // -> store a slice and continue with storing the remaining data
                                chunk = value.substr(startPos, maxChunkSize);

                                _storeSegment(id, chunk, partNo, self._dbInfo)
                                .then(saveInternal, function(error){
                                    reject(error);
                                });
                            } else {
                                // Last chunk
                                chunk = value.substr(startPos);

                                _storeSegment(id, chunk, partNo, self._dbInfo)
                                .then(function(){
                                    resolve();
                                },function(error){
                                    reject(error);
                                });
                            }
                            partNo++;
                            startPos += maxChunkSize;
                        };
                        saveInternal();
                    }
                });

            }).catch(reject);
        });

        if (callback) {
            promise.then(function(result) {
                callback(null, result);
            }, function(error) {
                callback(error);
            });
        }
        return promise;

    }



    var chunkedSQLStorage = {
        _driver: 'chunkedSQLStorage',
        _initStorage: _initStorage,
        iterate: iterate,
        getItem: getItem,
        setItem: setItem,
        removeItem: removeItem,
        clear: clear,
        length: length,
        key: key,
        keys: keys
    };

    if (typeof define === 'function' && define.amd) {
        define('chunkedSQLStorage', function() {
            return chunkedSQLStorage;
        });
    } else if (typeof module !== 'undefined' && module.exports) {
        module.exports = chunkedSQLStorage;
    } else {
        this.chunkedSQLStorage = chunkedSQLStorage;
    }
}).call(window);
