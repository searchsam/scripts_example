# Ruby Daru

## System dependencies

- libpq-dev
- libmysqlclient-dev o libmariadb-dev

## Get credentials

Copy `credentials_example.rb` into `.credentials.rb`:

```bashscript
cp credentials_example.rb .credentials.rb
```

and fill with youru data.

## Install dependences

```bashscript
bundle install
```

## Run script

```bashscript
ruby migration.rb
```
