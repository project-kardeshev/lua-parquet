package = "lua-parquet"
version = "1.0-1"
rockspec_format = "3.0"
source = {
   url = "./src/parquet.lua"
}
description = {
   summary = "A minimal Parquet v1.0 file writer implemented in pure Lua",
   detailed = [[
      Pure Lua implementation of a Parquet v1.0 file writer.
      Supports writing tables with INT32 and BYTE_ARRAY columns with plain encoding.
      No external dependencies beyond Lua 5.3+.
   ]],
   homepage = "https://github.com/project-kardeshev/lua-parquet",
   license = "MIT"
}
dependencies = {
 "busted >= 2.2.0",
    "luacov >= 0.15.0",
    "luacheck >= 1.1.2",
    "luacov-html >=1.0.0"
}
build = {
   type = "builtin",
   modules = {
      ["parquet"] = "src/parquet.lua",
      ["parquet.utils"] = "src/parquet/utils.lua",
      ["parquet.thrift"] = "src/parquet/thrift.lua",
      ["parquet.schema"] = "src/parquet/schema.lua",
      ["parquet.encoding"] = "src/parquet/encoding.lua",
      ["parquet.writer"] = "src/parquet/writer.lua"
   }
}
test = {
  type = "busted",
}