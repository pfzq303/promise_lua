local Promise = class("Promise")
local PromiseState = {
    pending = 0,
    fulfilled = 1,
    rejected = 2,
    waiting = 3,
}

local LAST_ERROR
local IS_ERROR = {} -- ��һ����������ʶ������
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

-- �ӳٵ���
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

--���ú������쳣ʱ��¼���󣬲����ش������
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

-- ��ȡ�����Then�����������ȡ��ʧ�ܣ���¼���󲢷��ش������
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

-- ִ��promise�Ĺ��캯����ȥ���ͳɹ�ʧ�ܵ�״̬�л���
doResolve = function (promise , fn)
    local done = false;
    -- ���ص���ֵ������ȥ�����
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

-- �� promise ��ɵ�ʱ��ִ�лص����ܴ����ֱ�Ӵ������ܴ�����ӳٵ�
handle = function (promise, deferred) 
    -- �����ǰ״̬��waiting״̬����ʾ����Ҫ�ȴ���������promiseִ�н���ʱ�ص�
    --��ȡ�������Ǹ�promise
	while (promise._state == PromiseState.waiting) do
		promise = promise._value;
	end
	if Promise._onHandle then
		Promise._onHandle(promise);
	end
    -- ����������Ǹ�promise���ڵȴ��У���ô��deferred�洢����
	if promise._state == PromiseState.pending then
        -- _deferredState �� 0��ʾ��ֵ��1 ��ʾ��ֵ��2��ʾ�б�
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
    -- ����ֱ�Ӵ���
	handleResolved(promise, deferred);
end

-- promise���սᴦ������ڶ����С����Ļص����ƽ��������Ǹ�promise
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

-- �����Ĵ���Then�ĵط������ϸ�promise�ķ���ֵ���ݵ���һ��
handleResolved = function (promise, deferred)
    delayCall(function()
        local cb = promise._state == PromiseState.fulfilled and deferred.onFulfilled or deferred.onRejected
        -- ��������ڴ���������ôֱ�ӳɹ�����ʧ��
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

-- �ɹ�״̬ʱ�Ĵ����߼�
resolve = function (promise , newValue)
    -- Promise��A+��׼�� https://github.com/promises-aplus/promises-spec#the-promise-resolution-procedure
    if promise == newValue then
        error("TypeError:A promise cannot be resolved with itself.")
        return
    end
    -- �������ֵ�Ǹ�Thenable�Ķ�����Ҫ���м�¼����
    if newValue and type(newValue) == 'table' then
        local t = getThen(newValue)
        if t == IS_ERROR then
            return reject(promise, LAST_ERROR)
        end
        --����Ǹ�Promise�Ķ��󡣣�ֱ�Ӽ̳л��Ӽ̳о��ɣ����ƽ��ص����л�״̬
        if iskindof(newValue , Promise) then
            promise._state = PromiseState.waiting
            promise._value = newValue
            finale(promise)
            return
        elseif type(t) == 'function' then
            -- ����Ǹ�������״̬���䡣��ôpromise�Ĵ���Ȩ���ƽ������������ִ�иú�����
            doResolve(promise, function(...)
                t(newValue , ...)
            end)
            return
        end
    end
    --����Thenable�Ķ���ֱ�ӳɹ�
    promise._state = PromiseState.fulfilled
    promise._value = newValue
    finale(promise)
end

--ʧ�ܵĴ���
reject = function (promise , reason)
    promise._state = PromiseState.rejected
    promise._value = reason
    if Promise._onReject then
        Promise._onReject(promise, reason)
    end
    finale(promise)
end

-- ����һ���ṹ�洢�ص�
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
-- Then����������ڳɹ���ʧ��ʱ�Ļص�����������һ���µ�promise��
-- ����µ�promise�ĳɹ���ʧ�ܺ������Ӧ�Ļص������йء�
-- ��������Ĵ����������һ��promise�ĳɹ��ص�ֵ��ִ�г������µ�promise reject, ����fulfill
function Promise:Then(onFulfilled , onRejected)
    local res = Promise.new()
    handle(self, Handler(res , onFulfilled, onRejected))
    return res
end

-- ��Then���ơ����ǲ������µ�promise,ͬʱ���׳��쳣
function Promise:done(onFulfilled , onRejected)
    local p = self:Then(onFulfilled, onRejected) or self
    p:Then(nil, function (err) 
        error( err )
    end)
end

-- �����쳣
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

-- ֱ��resolve����ֵ����һ���������promise����Thenable�Ļ�������promise������
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

-- ִ�����е�promise�������н��ִ����ɺ�һ�𷵻�
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
            -- ִ�е�����˵���Ѿ������ִ��
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

-- ֱ�ӷ���һ��rejected��promise
Promise.reject = function (value) 
    return Promise.new(function (resolve, reject) 
        reject(value);
    end)
end

-- һ��ĳ��promise�����ܾ������ص� promise�ͻ�����ܾ�
Promise.race = function (values) 
    return Promise.new(function (resolve, reject) 
        for value , v in ipairs(values) do
            Promise.resolve(value):Then(resolve, reject)
        end
    end)
end

--finally ��������һ��Promise����ִ��then��catch�󣬶���ִ��finallyָ���Ļص�����
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