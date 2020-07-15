#!/usr/bin/env ruby
# frozen_string_literal: true

require 'daru'
require 'mysql2'
require '.credentials'

include Credentials

@client = Mysql2::Client.new(
  host: MY_HOST,
  database: MY_DB,
  username: MY_USER,
  password: MY_PASS
)

if __FILE__ == $PROGRAM_NAME
  df = Daru::DataFrame.from_sql(@client, 'SELECT * FROM users')
  puts df
end
