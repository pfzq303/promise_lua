local Promise = class("Promise")
local PromiseState = {
    pending = 0,
    fulfilled = 1,
    rejected = 2,
    waiting = 3,
}

local LAST_ERROR
local IS_ERROR = {} -- 用一个对象来标识错误结果
local _queue = {}

local tryCall
local getThen
local doResolve
local handle
local finale
local delayCall
local handleResolved
local resolve
local reject
local Handler
local safeThen

-- 延迟调用
delayCall = function (func)
    _queue[#_queue + 1] = func
    if #_queue == 1 then
        scheduler.performWithDelayGlobal(function ()
            for _ , v in ipairs(_queue) do
                v()
            end
            _queue = {}
        end, 0)
    end
end

--调用函数。异常时记录错误，并返回错误对象
tryCall = function(fn, ...)
    local args = {...}
    local status , ret = xpcall(function()
        return fn(unpack(args))
    end, function(ex)
        LAST_ERROR = ex
    end)
    if status then
        return ret
    else
        return IS_ERROR
    end
end

-- 获取对象的Then函数。如果获取中失败，记录错误并返回错误对象
getThen = function (obj)
    local status , ret = xpcall(function()
        return obj.Then
    end, function(ex)
        LAST_ERROR = ex;
    end)
    if status then
        return ret
    else
        return IS_ERROR
    end
end

-- 执行promise的构造函数。去发送成功失败的状态切换。
doResolve = function (promise , fn)
    local done = false;
    -- 将回调的值传递下去并标记
    local ret = tryCall(fn, function (value)
        if (done) then return end
        done = true
        resolve(promise, value)
    end, function (reason) 
        if (done) then return end
        done = true
        reject(promise, reason)
    end)
    if not done and ret == IS_ERROR then
        done = true
        reject(promise, LAST_ERROR)
    end
end

-- 在 promise 完成的时候执行回调，能处理就直接处理。不能处理就延迟到
handle = function (promise, deferred) 
    -- 如果当前状态是waiting状态。表示他需要等待到他的子promise执行结束时回调
    --获取到最后的那个promise
	while (promise._state == PromiseState.waiting) do
		promise = promise._value;
	end
	if Promise._onHandle then
		Promise._onHandle(promise);
	end
    -- 如果是最后的那个promise还在等待中，那么将deferred存储起来
	if promise._state == PromiseState.pending then
        -- _deferredState ： 0表示无值。1 表示单值。2表示列表
		if promise._deferredState == 0 then
			promise._deferredState = 1
			promise._deferreds = deferred
			return;
		end
		if promise._deferredState == 1 then
			promise._deferredState = 2
			promise._deferreds = {promise._deferreds, deferred }
			return
		end
		table.insert(promise._deferreds , deferred)
		return
	end
    -- 否则直接处理
	handleResolved(promise, deferred);
end

-- promise的终结处。如果在队列中。他的回调会移交给最后的那个promise
finale =  function (promise)
	if promise._deferredState == 1 then
		handle(promise, promise._deferreds);
		promise._deferreds = null;
	end
	if promise._deferredState == 2 then
        for _ , v in ipairs(promise._deferreds) do
			handle(promise, promise._deferreds[i]);
		end
		promise._deferreds = null;
	end
end

-- 真正的处理Then的地方，将上个promise的返回值传递到下一个
handleResolved = function (promise, deferred)
    delayCall(function()
        local cb = promise._state == PromiseState.fulfilled and deferred.onFulfilled or deferred.onRejected
        -- 如果不存在处理函数，那么直接成功或者失败
        if not cb then
            if promise._state == PromiseState.fulfilled then
                resolve(deferred.promise, promise._value)
            else
                reject(deferred.promise, promise._value)
            end
            return
        end
        local ret = tryCall(cb, promise._value)
        if ret == IS_ERROR then
            reject(deferred.promise, LAST_ERROR)
        else
            resolve(deferred.promise, ret)
        end
    end)
end

-- 成功状态时的处理逻辑
resolve = function (promise , newValue)
    -- Promise的A+标准： https://github.com/promises-aplus/promises-spec#the-promise-resolution-procedure
    if promise == newValue then
        error("TypeError:A promise cannot be resolved with itself.")
        return
    end
    -- 如果返回值是个Thenable的对象需要进行记录起来
    if newValue and type(newValue) == 'table' then
        local t = getThen(newValue)
        if t == IS_ERROR then
            return reject(promise, LAST_ERROR)
        end
        --如果是个Promise的对象。（直接继承或间接继承均可），移交回调并切换状态
        if iskindof(newValue , Promise) then
            promise._state = PromiseState.waiting
            promise._value = newValue
            finale(promise)
            return
        elseif type(t) == 'function' then
            -- 如果是个函数。状态不变。那么promise的触发权利移交给这个函数，执行该函数。
            doResolve(promise, function(...)
                t(newValue , ...)
            end)
            return
        end
    end
    --不是Thenable的对象。直接成功
    promise._state = PromiseState.fulfilled
    promise._value = newValue
    finale(promise)
end

--失败的处理
reject = function (promise , reason)
    promise._state = PromiseState.rejected
    promise._value = reason
    if Promise._onReject then
        Promise._onReject(promise, reason)
    end
    finale(promise)
end

-- 创建一个结构存储回调
Handler = function(promise , onFulfilled, onRejected)
    local ret = {}
    ret.onFulfilled = type( onFulfilled ) == 'function' and onFulfilled
    ret.onRejected = type( onRejected ) == 'function' and onRejected
    ret.promise = promise
    return ret
end

function Promise:ctor(func)
    self._deferredState = 0
    self._state = PromiseState.Pending
    self._value = nil
    self._deferreds = nil
    if not func then return end
    if type(func) ~= "function" then
        error("new promise must be function")
        return
    end
    doResolve(self , func )
end
-- Then是用来添加在成功、失败时的回调。并创建出一个新的promise。
-- 这个新的promise的成功和失败和这个对应的回调处理有关。
-- 如果函数的传入参数是上一个promise的成功回调值。执行出错则新的promise reject, 否则fulfill
function Promise:Then(onFulfilled , onRejected)
    local res = Promise.new()
    handle(self, Handler(res , onFulfilled, onRejected))
    return res
end

-- 和Then类似。但是不返回新的promise,同时会抛出异常
function Promise:done(onFulfilled , onRejected)
    local p = self:Then(onFulfilled, onRejected) or self
    p:Then(nil, function (err) 
        error( err )
    end)
end

-- 捕获异常
function Promise:catch(onRejected) 
    return self:Then(nil, onRejected)
end

local function valuePromise(value) 
    local p = Promise.new()
    p._state = PromiseState.fulfilled
    p._value = value
    return p
end

local TRUE = valuePromise(true);
local FALSE = valuePromise(false);
local NULL = valuePromise(nil);
local ZERO = valuePromise(0);
local EMPTYSTRING = valuePromise('');

-- 直接resolve传递值到下一步，如果是promise或者Thenable的话，按照promise来传递
Promise.resolve = function (value) 
    if iskindof(value , Promise) then return value end

    if value == nil then return NULL end
    if value == true then return TRUE end
    if value == false then return FALSE end
    if value == 0 then return ZERO end
    if value == '' then return EMPTYSTRING end

    if type(value) == 'table'  then
        local t = value.Then
        if type(t) == 'function' then
            return Promise.new(function(...)
                t(value , ...)
            end);
        end
    end
    return valuePromise(value);
end

-- 执行所有的promise。等所有结果执行完成后。一起返回
Promise.all = function (arr)
    return Promise.new(function (resolve, reject) 
        if #arr == 0 then return resolve({}) end
        local args = {}
        local remaining = #arr
        function res(i, val) 
            if val and type(val) == 'object' then
                if iskindof(val , Promise) then
                    while val._state == PromiseState.waiting do
                        val = val._value
                    end
                    if val._state == PromiseState.fulfilled then return res(i, val._value) end
                    if val._state == PromiseState.rejected then reject(val._value) end
                    val:Then(function (val) 
                        res(i, val)
                    end, reject)
                    return
                else
                    local Then = val.Then
                    if type(Then) == 'function' then
                        local p = new Promise(function(...)
                            Then(val , ...)
                        end);
                        p.Then(function (val) 
                            res(i, val)
                        end, reject)
                        return
                    end
                end
            end
            -- 执行到这里说明已经完成了执行
            args[i] = val;
            remaining = remaining - 1
            if remaining == 0 then
                resolve(args)
            end
        end
        for i = 1 , #arr do
            res(i, arr[i])
        end
    end)
end

-- 直接返回一个rejected的promise
Promise.reject = function (value) 
    return Promise.new(function (resolve, reject) 
        reject(value);
    end)
end

-- 一旦某个promise解决或拒绝，返回的 promise就会解决或拒绝
Promise.race = function (values) 
    return Promise.new(function (resolve, reject) 
        for value , v in ipairs(values) do
            Promise.resolve(value):Then(resolve, reject)
        end
    end)
end

--finally 方法返回一个Promise，在执行then和catch后，都会执行finally指定的回调函数
function Promise:finally(f) 
    return self:Then(function (value) 
        return Promise.resolve(f()):Then(function () 
            return value
        end)
    end, function (err) 
        return Promise.resolve(f()):Then(function () 
            error(err)
        end)
    end)
end

function Promise:getState() 
    if self._state == PromiseState.waiting then
        return self._value:getState()
    end
    if self._state == PromiseState.pending 
        or self._state ==  PromiseState.fulfilled
        or self._state ==  PromiseState.rejected then
        return self._state;
    end
    return PromiseState.pending
end

function Promise:isPending() 
    return self:getState() == PromiseState.pending
end

function Promise:isFulfilled() 
    return self:getState() == PromiseState.fulfilled
end

function Promise:isRejected() 
    return self:getState() == PromiseState.rejected
end

function Promise:getValue() 
    if self._state == PromiseState.waiting then
      return self._value:getValue()
    end

    if not self:isFulfilled() then
        error('Cannot get a value of an unfulfilled promise.')
        return
    end
    return self._value
end

function Promise:getReason() 
    if self._state == PromiseState.waiting then
      return self._value:getReason()
    end

    if not self:isRejected() then
        error('Cannot get a value of a non-rejected promise.')
        return
    end
    return self._value
end


return Promise