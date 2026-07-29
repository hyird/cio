local request_bytes = wrk.format()
local batch = {}

for i = 1, 256 do
  batch[i] = request_bytes
end

local pipelined = table.concat(batch)

request = function()
  return pipelined
end
