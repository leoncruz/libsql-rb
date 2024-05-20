# Libsql-rb

A simple wrapper around the Libsql HTTP protocol. Does not support the Libsql protocol,
only the HTTP method based on the [documentation](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_2_SPEC.md)

## Installation

    $ gem install libsql-rb

## Usage

#### Example

```ruby
client = Libsql::Client.new host: "http://localhost:8080"

client.execute "create table users(name, email, password)"

# Is possible to pass positional arguments
client.execute "insert into users(name, email, password) values(?, ?, ?)", ["User 1", "user1@email..com", "1111111"]

# or named aguments
client.execute "insert into users(name, email, password) values(:name, :email, :password)", ["User 1", "user1@email..com", "1111111"]

result = client.execute "select * from users"

result.rows # get the rows as Struct
```
