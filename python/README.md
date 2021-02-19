# Python Pandas

## System dependencies

### Fedora

- python3-devel

## Get credentials

Copy `env_example.cfg` into `.env`:

```bashscript
$ cp env_example .env
```

and fill with youru data.

## Activate virtual environtment

```bashscript
$ python -m venv .venv
$ source .venv/bin/activate
```

## Install dependences

```bashscript
(.venv)$ pip install -r requirements.txt
```

## Run script

```bashscript
(.venv)$ python migration.py
```

## Deactivate virtual environtment

```bashscript
(.venv)$ deactivate
```
