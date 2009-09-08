

-- Tokyo Tyrant interface for Lua 5.1
-- Phoenix Sol -- phoenix@burninglabs.com
-- ( mostly translated from Mikio's Ruby interface ) --
-- Thanks, Mikio.

--[[
Copyright 2009 Phoenix Sol (aka Corey Michael Trampe)

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
]]--


local struct = require 'struct'
local nio = require 'nixio', require 'nixio.util'
local shl, shr = nio.bit.lshift, nio.bit.rshift

local function module(name) end --trick luadoc
module 'tokyotyrant'

--
--- Remote Database Object
--
local RDB = { MAGIC = 0xC8, --a little can go a long way

              MONOULOG = 1, --ommit update log (misc function)
              XOLCKREC = 1, --record locking (lua extension)
              XOLCKGLB = 2, --global locking (lua extension)
                 
              PUT = 0x10,
              PUTKEEP = 0x11,
              PUTCAT = 0x12,
              PUTSHL = 0x13,
              PUTNR = 0x18,
              OUT = 0x20,
              GET = 0x30,
              MGET = 0x31,
              VSIZ = 0x38,
              ITERINIT = 0x50,
              ITERNEXT = 0x51,
              FWMKEYS = 0x58,
              ADDINT = 0x60,
              ADDDOUBLE = 0x61,
              EXT = 0x68,
              SYNC = 0x70,
              OPTIMIZE = 0x71,
              VANISH = 0x72,
              COPY = 0x73,
              RESTORE = 0x74,
              SETMST = 0x78,
              RNUM = 0x80,
              SIZE = 0x81,
              STAT = 0x88,
              MISC = 0x90,

              ESUCCESS = 'success',
              EINVALID = 'invalid operation',
              ENOHOST  = 'host not found',
              EREFUSED = 'connection refused',
              ESEND = 'send error',
              ERECV = 'recv error',
              EKEEP = 'existing record',
              ENOREC = 'no record found',
              EMISC  = 'miscellaneous error',

              _sockerr = function(self, code)
                if code == -2 or code == -5 then return self.ENOHOST
                elseif code == 111 then return self.EREFUSED end
              end,

              _recvuchar = function(self)
                return struct.unpack('>B', self.sock:recv(1)) or -1
              end,

             _recvint32 = function(self)
               return struct.unpack('>i4', self.sock:readall(4)) or -1
             end,

             _recvint64 = function(self)
               return struct.unpack('>i8', self.sock:readall(8)) or -1
             end,

             _packquad = function(self, num)
               --please let me know if you have a better solution
               local high = math.floor( num / (shl(1, 32)) )
               local low = math.fmod( num, (shl(1, 32)) )
               return struct.pack('>i8i8', high, low)
             end,
            } 

---initialize a new Remote Database Object
--'__call'  metamethod of RDB
--@return  new RDB Object
--@usage  tyr = require'tokyotyrant'; rdb = tyr.RDB(); rdb.open(host, port)
function RDB:new()
  local inst = {}
  setmetatable(inst, self)
  self.__index = function(t,k)
    return rawget(RDB, k) or t:get(k)
  end
  self.__newindex = function(t,k,v)
    return t:put(k,v)
  end
  return inst
end

setmetatable(RDB, {__call = RDB.new})

---open a remote database connection
--@param host  the host string. (defaults to localhost)
--@param port  the port number (number or string) (defaults to '1978')
--@return  true or false, error message
function RDB:open(host, port)
  --TODO support UNIX domain sockets
  if rawget(self, sock) then return false, self.EINVALID end
  if self == RDB then return false, self.EINVALID end
  --a nil host = 'localhost' in nixio.connect
  local port = port or '1978'
  local sock, ercode = nio.connect(host, port)
  if not sock then return false, self.sockerr(ercode) end
  sock:setopt('tcp', 'nodelay', 1)
  rawset(self, 'sock', sock)
end

---close remote database connection
--@return true or false, error message
function RDB:close()
  if not self.sock then return false, self.EINVALID end
  if self.sock:close() == true then
    rawset(sock, nil)
    return true
  else return false, self.EMISC end
end

---store a record
--if a record with same key already exists then it is overwritten
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false, error message
function RDB:put(key, val)
  if not self.sock then return false, self.EINVALID end
  local key, val = tostring(key), tostring(val)
  local req = struct.pack('>BBi4i4', self.MAGIC, self.PUT, #key, #val)
  if not self.sock:writeall(req .. key .. val) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---store a new record
--if record with same key exists then this has no effect
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false, error message
function RDB:putkeep(key, val)
  if not self.sock then return false, self.EINVALID end
  local key, val = tostring(key), tostring(val)
  local req = struct.pack('>BBi4i4', self.MAGIC, self.PUTKEEP, #key, #val)
  if not self.sock:writeall(req .. key .. val) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EKEEP end
  return true
end

---concatenate a value at the end of an existing record
--if no record with given key exists then create new record
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false, error message
function RDB:putcat(key, val)
  if not self.sock then return false, self.EINVALID end
  local key, val = tostring(key), tostring(val)
  local req = struct.pack('>BBi4i4', self.MAGIC, self.PUTCAT, #key, #val)
  if not self.sock:writeall(req .. key .. val) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---concatenate a value at the end of an existing record and
--shift it left by (length of concatenation result - provided width)
--if no record with given key exists then create new record
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@param width  the desired record length
--@return  true or false, error message
function RDB:putshl(key, val, width)
  if not self.sock then return false, self.EINVALID end
  local key, val = tostring(key), tostring(val)
  local width = tonumber(width) or 0
  local req = struct.pack('>BBi4i4i4', self.MAGIC, self.PUTSHL, #key, #val, width)
  if not self.sock:writeall(req .. key .. val) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---store a record, with no response from the server
--if record with same key already exists then it is overwritten
--@param key  the record key (coerced to string)
--@param val  the record value (coerced to string)
--@return  true or false, error message
function RDB:putnr(key, val)
  if not self.sock then return false, self.EINVALID end
  local key, val = tostring(key), tostring(val)
  local req = struct.pack('>BBi4i4', self.MAGIC, self.PUTNR, #key, #val)
  if not self.sock:writeall(req .. key .. val) then
    return false, false, self.ESEND end
  return true
end

---remove a record
--@param key  the record key (coerced to string)
--@return  true or false, error message
function RDB:out(key)
  if not self.sock then return false, self.EINVALID end
  local key = tostring(key)
  local req = struct.pack('>BBi4', self.MAGIC, self.OUT, #key)
  if not self.sock:writeall(req .. key) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.ENOREC end
  return true
end

---retrieve a record
--@param key  the record key (coerced to string)
--@return  record value or nil, error message
function RDB:get(key)
  if not self.sock then return false, self.EINVALID end
  local key = tostring(key)
  local req = struct.pack('>BBi4', self.MAGIC, self.GET, #key)
  if not self.sock:writeall(req .. key) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.ENOREC end
  local vsiz = self:_recvint32()
  if vsiz < 0 then return nil, self.ERECV end
  local vbuf = self.sock:readall(vsiz)
  if not vbuf then return nil, self.ERECV end
  return vbuf
end

---retrieve multiple records
--given a table containing an array of keys, add a hash of key-value pairs
--keys in the array with no corresponding value will be removed from the array
--( understand that the table given is modified in place )
--@param  recs an array of keys
--@return  number of retrieved records or -1, error message
function RDB:mget(recs)
  if not self.sock then return -1, self.EINVALID end
  if not type(recs) == 'table' then return -1, self.EINVALID end
  local req = ''
  for i, k in ipairs(recs) do
    req = req .. struct.pack('>i4', #k) .. k
  end
  req = struct.pack('>BBi4', self.MAGIC, self.MGET, #recs) .. req
  if not self.sock:writeall(req) then return -1, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return -1, self.ERECV end
  if code ~= 0 then return -1, self.ENOREC end
  local rnum = self:_recvint32()
  if rnum < 0 then return -1, self.ERECV end
  local ksiz, vsiz, kbuf, vbuf = 0, 0, '', ''
  for i = 1, rnum do
    ksiz, vsiz = self:_recvint32(), self:_recvint32()
    if ksiz < 0 or vsiz < 0 then return -1, self.ERECV end
    kbuf, vbuf = self.sock:readall(ksiz), self.sock:readall(vsiz)
    if not kbuf or not vbuf then return -1, self.ERECV end
    recs[kbuf] = vbuf
  end
  return rnum
end

---get the size of a record value
--@param key  the record key (coerced to string)
--@return  size or -1, error message
function RDB:vsiz(key)
  if not self.sock then return -1, self.EINVALID end
  local key = tostring(key)
  local req = struct.pack('>BBi4', self.MAGIC, self.VSIZ, #key)
  if not self.sock:writeall(req .. key) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return -1, self.ERECV end
  if code ~= 0 then return -1, self.ENOREC end
  return self:_recvint32()
end

---initialize the iterator (used to access the key of every record)
--@return  true or false, error code
function RDB:iterinit()
  if not self.sock then return false, self.EINVALID end
  local req = struct.pack('>BB', self.MAGIC, self.ITERINIT)
  if not self.sock:writeall(req) then return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---get the next key of the iterator
--the iterator will traverse the database in arbitrary order
--[[ It is possible to access every record by iteration of calling this method. It is allowed to update or remove records whose keys are fetched while the iteration. However, it is not assured if updating the database is occurred while the iteration. Besides, the order of this traversal access method is arbitrary, so it is not assured that the order of storing matches the one of the traversal access. ]]--
--@return  next key or nil, error message
function RDB:iternext()
  if not self.sock then return nil, self.EINVALID end
  local req = struct.pack('>BB', self.MAGIC, self.ITERNEXT)
  if not self.sock:writeall(req) then return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.ENOREC end
  local vsiz = self:_recvint32()
  if vsiz < 0 then return nil, self.ERECV end
  local vbuf = self.sock:readall(vsiz)
  if not vbuf then return nil, self.ERECV end
  return vbuf
end

---get forward matching keys
--note this will scan EVERY KEY in the database and may be  s l o w
--@param prefix  prefix of corresponding keys
--@param max  max number of keys to fetch
--@return  array of keys of corresponding records or {}, error message
function RDB:fwmkeys(prefix, max)
  if not self.sock then return {}, self.EINVALID end
  prefix = tostring(prefix)
  max = max or -1
  local req = struct.pack('>BBi4i4', self.MAGIC, self.FWMKEYS, #prefix, max)
  if not self.sock:writeall(req .. prefix) then return {}, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return {}, self.ERECV end
  if code ~= 0 then return {}, self.ENOREC end
  local knum = self:_recvint32()
  if knum < 0 then return {}, self.ERECV end
  local keys = {}
  local ksiz, kbuf = 0, ''
  for i = 1,knum do
    ksiz = self:_recvint32()
    if ksiz < 0 then return {}, self.ERECV end
    kbuf = self.sock:readall(ksiz)
    if not kbuf then return {}, self.ERECV end
    keys[#keys+1] = kbuf
  end
  return keys
end

---add an integer to a record
--if record exists, it is treated as an integer and added to
--else a new record is created with the provided value
--records are stored in binary format, and must be unpacked upon retrieval
--@param key  the record key
--@param num  the additional value. (defaults to 0)
--@return  sum or nil, error message
function RDB:addint(key, num)
  if not self.sock then return nil, self.EINVALID end
  local key = tostring(key)
  local num = tonumber(num) or 0
  local req = struct.pack('>BBi4i4', self.MAGIC, self.ADDINT, #key, num)
  if not self.sock:writeall(req .. key) then return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.EKEEP end
  return self:_recvint32()
end

---add a real number to a record
--if record exists, it is treated as a real number and added to
--else a new record is created with the provided value
--records are stored in binary format, and must be unpacked upon retrieval
--@param key  the record key
--@param num  the additional value. (defaults to 0)
--@return  sum or nil, error message
function RDB:adddouble(key, num)
  --XXX  I doubt that this is correct
  if not self.sock then return nil, self.EINVALID end
  local key = tostring(key)
  local num = tonumber(num) or 0
  local integ = math.floor(num)
  local fract = math.floor( (num - integ) * 1000000000000 )
  local req = struct.pack('>BBi4', self.MAGIC, self.ADDOUBLE, #key)
  req = req + self:_packquad(integ) + self:_packquad(fract) + key
  if not self.sock:writeall(req) then return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.EKEEP end
  return (self:_recvint32() + self:_recvint32()) / 1000000000000.0
end

---call a function of the server-side lua extension
--@param name  the function name
--@param key  the key (defaults to empty string)
--@param val  the value (defaults to empty string)
--@param opts  options by bitwise-or
--option RDB.XOLCKREC = record locking
--option RDB.XOLCKGLB = global locking
--@return  response or nil, error message
function RDB:ext(name, key, val, opts)
  if not self.sock then return nil, self.EINVALID end
  local name = tostring(name)
  local key = tostring(key) or ""
  local val = tostring(val) or ""
  local opts = tonumber(opts) or 0
  local req = struct.pack('>BBi4i4i4i4', self.MAGIC, self.EXT,
                                     #name, opts, #key, #val)
  if not self.sock:writeall(req .. name) then return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.EMISC end
  local vsiz = self:_recvint32()
  if vsiz < 0 then return nil, self.ERECV end
  local vbuf = self.sock:readall(vsiz)
  if not vbuf then return nil, ERECV end
  return vbuf
end

---synchronize updated contents with the file and device
--@return  true or false, error message
function RDB:sync()
  if not self.sock then return false, self.EINVALID end
  local req = struct.pack('>BB', self.MAGIC, self.SYNC)
  if not self.db:writeall(req) then return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---optimize storage according to provided tuning params
--@return  true or false, error message
function RDB:optimize(params)
  if not self.sock then return false, self.EINVALID end
  local params = tostring(params) or ''
  local req = struct.pack('>BBi4', self.MAGIC, self.OPTIMIZE, #params)
  if not self.db:writeall(req) then return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---remove all records
--@return  true or false, error message
function RDB:vanish()
  if not self.sock then return false, self.EINVALID end
  if not self.sock:writeall( struct.pack('>BB', self.MAGIC, self.VANISH) ) then
    return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

---copy the database file to provided file path
--the db file will be kept in sync and not modified during copy
--@param path  the file path to copy to
--if path begins with '@' then trailing substring is executed as command line
--@return  true or false, error message
function RDB:copy(path)
  if not self.sock then return false, self.EINVALID end
  if not path then return false, self.EINVALID end
  local path = tostring(path)
  local req = struct.pack('>BBi4', self.MAGIC, self.COPY, #path)
  if not self.sock:writeall(req .. path) then return false, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return false, self.ERECV end
  if code ~= 0 then return false, self.EMISC end
  return true
end

-- TODO: Missing protocol function "restore" for the function `tcrdbrestore'
-- TODO: Missing protocol function "setmst" for the function `tcrdbsetmst'

---get number of records
--( limited to Lua's number type precision )
--@return  record number or 0, error message
function RDB:rnum()
  if not self.sock then return 0, self.EINVALID end
  if not self.sock:writeall( struct.pack('>BB', self.MAGIC, self.RNUM) ) then
    return 0, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return 0, self.ERECV end
  if code ~= 0 then return 0, self.EMISC end
  local rv = self:_recvint64()
  if rv < 0 then return 0, self.ERECV end
  return rv
end

---get size of database
--( limited to Lua's number type precision )
--@return  record size or 0, error message
function RDB:size()
  if not self.sock then return 0, self.EINVALID end
  if not self.sock:writeall( struct.pack('>BB', self.MAGIC, self.SIZE) ) then
    return 0, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return 0, self.ERECV end
  if code ~= 0 then return 0, self.EMISC end
  local rv = self:_recvint64()
  if rv < 0 then return 0, self.ERECV end
  return rv
end

---get status string from remote database server
--( string is in 'tab separated values' format )
--@return  status string or nil, error message
function RDB:stat()
  if not self.sock then return nil, self.EINVALID end
  if not self.sock:writeall( struct.pack('>BB', self.MAGIC, self.STAT) ) then
    return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.EMISC end
  local ssiz = self:_recvint32()
  if ssiz < 0 then return nil, self.ERECV end
  return self.sock.readall(ssiz) or nil, self.ERECV
end

---call a versatile function for miscellaneous operations
--@param name  one of 'putlist', 'getlist', or 'outlist' for any type of db
--or 'setindex', 'search', or 'genuid' for table db
--@param args  an array containing arguments
--@param opts  a bitmask containing options
--@return  array of results or nil, error message
function RDB:misc(name, args, opts)
  if not self.sock then return nil, self.EINVALID end
  local name = tostring(name)
  local args = args or {}
  local opts = opts or 0
  local req = struct.pack('>BBi4i4i4', self.MAGIC, self.MISC, #name, opts, #args) 
  req = req .. name
  for i, arg in ipairs(args) do
    arg = tostring(arg)
    req = req .. struct.pack('>i4', #arg) .. arg
  end
  if not self.sock:writeall(req) then return nil, self.ESEND end
  local code = self:_recvcode()
  if code == -1 then return nil, self.ERECV end
  if code ~= 0 then return nil, self.EMISC end
  local rnum = self:_recvint32()
  local res, esiz, ebuf = {}, 0, ''
  for i=1, rnum do
    esiz = self:_recvint32()
    if esiz < 0 then return nil, self.ERECV end
    ebuf = self.sock:readall(esiz)
    if not ebuf then return nil, self.ERECV end
    res[#res+1] = ebuf
  end
  return res
end

--
--aliases and iterators
--
RDB.connect = RDB.open
RDB.store = RDB.put
RDB.delete = RDB.out
RDB.fetch = RDB.get
RDB.clear = RDB.vanish
RDB.length = RDB.rnum

---test for key
--@param key  key
--@return  boolean
function RDB:has_key(key)
  return self:vsiz(key) >= 0
end

---test for value
--@param val  value
--@return  boolean
function RDB:has_value(val)
  if not self:iterinit() then return nil end
  local tkey, tval = '', ''
  repeat
    tkey = self:iternext()
    tval = self:get(tkey)
    if not tval then break end
    if tval == val then return true end
  until not tkey
end

---test if database is empty
--@return  boolean
function RDB:is_empty()
  return self:rnum() < 1
end

---iterate over each key-value pair in a database
--( simple iterator; state is held in remote database )
--@return  function
function RDB:each()
  if not self:iterinit() then return nil end
  return function()
    local k = self:iternext()
    return k, self:get(k)
  end
end

--you know you need this.
RDB.each_pair = RDB.each

---iterate over each key in a database
--( simple iterator; state is held in remote database )
--@return  function
function RDB:each_keys()
  if not self:iterinit() then return nil end
  return function() return self:iternext() end
end

---return an array of all keys
--@return  table
function RDB.keys()
  local tkeys = {}
  for k in self:each_keys() do tkeys[#tkeys+1] = k end
  return tkeys
end

---iterate over each value in the database
--( simple iterator; state is held in remote database )
--@return  function
function RDB:each_values()
  return function() return self:get( self:iternext() ) end
end

---return an array of all values
--@return  table
function RDB.values()
  local tvals = {}
  for v in self:each_values() do tvals[#tvals+1] = v end
  return tvals
end

--
--- Remote Table Database Object
--
local RDBTBL = { ITLEXICAL = 0,
                 ITDECIMAL = 1,
                 ITTOKEN = 2,
                 ITQGRAM = 3,
                 ITOPT   = 9998,
                 ITVOID  = 9999,
                 ITKEEP  = shl(1, 24)
               }

---initialize a new Remote Table Database Object
--'__call' metamethod of RDBTBL
--@return  new RDBTBL Object
function RDBTBL:new()
  local inst = {}
  setmetatable(inst, self)
  self.__index = function(t,k)
    return self[k] or RDB[k]  --inherit from RDB
  end
  return inst
end

setmetatable(RDBTBL, {__call = RDBTBL.new})

---store a record
--( overwrite if key exists )
--@param pkey  the primary key
--@param cols  table of columns
--@return  true or false, error message
function RDBTBL:put(pkey, cols)
  local pkey = tostring(pkey)
  if type(cols) ~= 'table' then error("'cols' must be a table of columns") end
  local args = {}
  for k,v in cols do
    args[#args+1] = k
    args[#args+1] = v
  end
  local res, err = self:misc("put", args, 0)
  if not res then return false, err
  else return true end
end

---store a record if key does not already exist, else do nothing
--@param pkey  the primary key
--@param cols  a table of columns
--@return  true or false, error message
function RDBTBL:putkeep(pkey, cols)
  local pkey = tostring(pkey)
  if type(cols) ~= 'table' then error("'cols' must be a table of columns") end
  local args = {}
  for k,v in cols do
    args[#args+1] = k
    args[#args+1] = v
  end
  local res, err = self:misc("putkeep", args, 0)
  if not res then return false, self.EKEEP
  else return true end
end

---concatenate columns of an existing record or create a new record
--@param pkey  primary key
--@param cols  a table of columns
--@return  true or false, error message
function RDBTBL:putcat(pkey, cols)
  local pkey = tostring(pkey)
  if type(cols) ~= 'table' then error("'cols' must be a table of columns") end
  local args = {}
  for k,v in cols do
    args[#args+1] = k
    args[#args+1] = v
  end
  local res, err = self:misc('putcat', args, 0)
  if not res then return false, err
  else return true end
end

---remove a record
--@param pkey  the primary key
--@return  true or false, error message
function RDBTBL:out(pkey)
  local pkey = tostring(pkey)
  local args = { pkey }
  local res, err = self:misc('out', args, 0)
  if not res then return false, self.ENOREC
  else return true end
end

---retrieve a record
--@param pkey  the primary key
--@return  a table of columns
function RDBTBL:get(pkey)
  local pkey = tostring(pkey)
  local args = { pkey }
  local res, err = self:misc('get', args)
  if not res then return nil, self.ENOREC end
  local cols = {}
  while #res > 0 do cols[table.remove(res, 1)] = table.remove(res, 1) end
  return cols
end

---retrieve multiple records
--given a table containing an array of keys, add a hash of key-value pairs
--( values being columns )
--keys in the array with no corresponding value will be removed from the array
--( understand that the table given is modified in place )
--NOTE: due to protocol restriction, this method cannot handle records with
--binary columns including the "\0" character.
--@param recs  an array of keys
--@return  number of retrieved records or -1, error message
function RDBTBL:mget(recs)
  local res, err = RDB.mget(self, recs)
  if res == -1 then return -1, err end
  for k,v in recs do
    local cols = {}
    local func, str = strsplit(v, '\%z')
    while true do
      local kk, vv = func(str), func(str)
      if kk then cols[kk] = vv
      else break end
    end
    recs[k] = cols
  end
  return res
end

---set a column index
--@param name  the column name.
--if the name of an existing index is specified, then the index is rebuilt.
--an empty string means the primary key.
--@param itype  the index type:
--         `TokyoTyrant::RDBTBL::ITLEXICAL' for lexical string,
--         `TokyoTyrant::RDBTBL::ITDECIMAL' for decimal string,
--         `TokyoTyrant::RDBTBL::ITTOKEN' for token inverted index,
--         `TokyoTyrant::RDBTBL::ITQGRAM' for q-gram inverted index,
--         `TokyoTyrant::RDBTBL::ITOPT' will optimize the index,
--         `TokyoTyrant::RDBTBL::ITVOID' will remove the index,
--         `TokyoTyrant::RDBTBL::ITKEEP', if added by bitwise OR and the index
--            exists, will merely return failure.
--@return  true or false
function RDBTBL:setindex(name, itype)
  return self:misc('setindex', { tostring(name), tostring(itype), 0 }) ~= nil
end

---generate a unique id number
--@return  unique id number or -1, error message
function RDBTBL:genuid()
  local res, err = self:misc('genuid', {}, 0)
  if not res then return -1, err
  else return res[0] end
end

--- Remote Database Query Object
--( helper class for RDBTBL )--
local RDBQRY = { --query conditions:
                 QCSTREQ = 0,   --string is equal to
                 QCSTRINC = 1,  --string is included in
                 QCSTRBW = 2,   --string begins with
                 QCSTREW = 3,   --string ends with
                 QCSTRAND = 4,  --string includes all tokens in
                 QCSTROR = 5,   --string includes at least one token in
                 QCSTROREQ = 6, --string is equal to at least one token in
                 QCSTRRX = 7,   --string matches regular expressions of
                 QCNUMEQ = 8,   --number is equal to
                 QCNUMGT = 9,   --number is greater than
                 QCNUMGE = 10,  --number is greater than or equal to
                 QCNUMLT = 11,  --number is less than
                 QCNUMLE = 12,  --number is less than or equal to
                 QCNUMBT = 13,  --number is between two tokens of
                 QCNUMOREQ = 14,--number is equal to at least one token in
                 QCFTSPH = 15,  --full-text search with the phrase of
                 QCFTSAND = 16, --full-text search with all tokens in
                 QCFTSOR = 17,  --full-text search with at least one token in
                 QCFTSEX = 18,  --f-text search with the compound expression of
                 QCNEGATE = shl(1,24), --negation flag
                 QCNOIDX = shl(1,25),  --no index flag
                 --order types:
                 QOSTRASC = 0, --string ascending
                 QOSTRDESC = 1,--string descending
                 QONUMASC = 2, --number ascending
                 QONUMDESC = 3,--number descending
               }

---initialize a new query object
--'__call' metamethod for RDBQRY
--@return  new RDBQRY object
function RDBQRY:new(rdb)
  self.rdb = rdb
  self.args = {}
end

setmetatable(RDBQRY, {__call = RDBQRY.new})

---add a narrowing condition
--@param name  specifies a column name.
--empty string indicates the primary key.
--@param op  specifies an operation type:
--QCSTREQ = 0,   --string is equal to
--QCSTRINC = 1,  --string is included in
--QCSTRBW = 2,   --string begins with
--QCSTREW = 3,   --string ends with
--QCSTRAND = 4,  --string includes all tokens in
--QCSTROR = 5,   --string includes at least one token in
--QCSTROREQ = 6, --string is equal to at least one token in
--QCSTRRX = 7,   --string matches regular expressions of
--QCNUMEQ = 8,   --number is equal to
--QCNUMGT = 9,   --number is greater than
--QCNUMGE = 10,  --number is greater than or equal to
--QCNUMLT = 11,  --number is less than
--QCNUMLE = 12,  --number is less than or equal to
--QCNUMBT = 13,  --number is between two tokens of
--QCNUMOREQ = 14,--number is equal to at least one token in
--QCFTSPH = 15,  --full-text search with the phrase of
--QCFTSAND = 16, --full-text search with all tokens in
--QCFTSOR = 17,  --full-text search with at least one token in
--QCFTSEX = 18,  --f-text search with the compound expression of
--all ops can be flagged by bitwise-or:
--QCNEGATE = shl(1,24), --negation flag
--QCNOIDX = shl(1,25),  --no index flag
--@expr  specifies an operand expression
--@return  nil
function RDBQRY:addcond(name, op, expr)
  self.args[#self.args+1]= 'addcond'..'\0'..name..'\0'..tostring(op)..'\0'..expr
  return nil
end

---set result order
--@param name  specifies column name. empty string indicates the primary key.
--@param otype  specifies the order type:
--QOSTRASC = 0, --string ascending
--QOSTRDESC = 1,--string descending
--QONUMASC = 2, --number ascending
--QONUMDESC = 3,--number descending
--@return  nil
function RDBQRY:setorder(name, otype)
  self.args[#self.args+1] = 'setorder'..'\0'..name..'\0'..tostring(otype)
  return nil
end

---set maximum number of records for the result
--@param max  the maximum number of records. nil or negative means no limit
--@param skip the number of skipped records. nil or !>0 means none skipped
--@return  nil
function RDBQRY:setlimit(max, skip)
  local max = max or -1
  local skip = skip or -1
  self.args[#self.args]= 'setlimit'..'\0'..tostring(max)..'\0'..tostring(skip)
  return nil
end

---execute the search
--@return  array of primary keys of corresponding records
--or {}, error message
function RDBQRY:search()
  local res, err = self.rdb:misc('search', self.args, 0)
  return res or {}, err
end

---remove each corresponding record
--@return true or false, error message
function RDBQRY:searchout()
  self.args[#self.args+1] = 'out'
  local i = #self.args
  local res, err = self.rdb:misc('search', args, 0)
  table.remove(self.args, i)
  return res ~= nil or false, err
end

---get records corresponding to search
--due to protocol restriction, method cannot handle records with binary cols
--including the '\0' character
--@param names  specifies an array of colum names to fetch.
--empty string means the primary key
--nil means fetch every column
--@return  array of column hashes of corresponding records
--or {}, error message
function RDBQRY:searchget(names)
  if type(names) ~= 'table' then
    error("'names' must be an array of column names")
  end
  local args = {}
  if #names > 0 then
    args[1] = "get\0" .. table.concat(names, '\0')
  else args[1] = "get" end
  local res, err = self.rdb:misc('search', args, RDB.MONOULOG)
  if not res then return {}, err end
  for i,v in res do
    local cols = {}
    local func, str = strsplit(v, '\%z')
    while true do
      local kk, vv = func(str), func(str)
      if kk then cols[kk] = vv
      else break end
    end
    res[i] = cols
  end
  return res
end

---get the count of corresponding records
--@return  count or 0, error message
function RDBQRY:searchcount()
  local res, err = self.rdb:misc('search', {'count'}, RDB.MONOULOG)
  if not res then return 0, err end
  return tonumber(res[1])
end

--------------------------------------------------------------------------------
---Rici Lake's string splitter
--( slightly modified so it doesn't get added to the global string table )--
local function strsplit(str, pat)
  local st, g = 1, str:gmatch("()("..pat..")")
  local function getter(str, segs, seps, sep, cap1, ...)
    st = sep and seps + #sep
    return str:sub(segs, (seps or 0) - 1), cap1 or sep, ...
  end
  local function splitter(str, self)
    if st then return getter(str, st, g()) end
  end
  return splitter, str
end

return { RDB=RDB, RDBTBL=RDBTBL } --All your database are belong to Mikio!

--
-- Essential attitude support provided *for free* by Trent Reznor.
-- [ http://nin.com ]
--

