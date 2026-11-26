# Contributing

- Python 3.11+; `python3 -m venv venv && venv/bin/pip install fastapi uvicorn httpx pydantic`
- Test: `./venv/bin/python -m unittest discover -s tests`
- Run: `./venv/bin/uvicorn server:app --host <tailscale-ip> --port 8081`
- Configure optional cloud models via the env vars in `.env.example`.
- Never commit `.env`, databases, logs or backups. Keep tests green.
