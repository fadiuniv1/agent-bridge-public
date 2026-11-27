## Summary
What does this change do, and why?

## Related issue
Closes #

## Type of change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Build / CI / installer

## Checklist
- [ ] `python -m unittest discover -s tests` passes (81 tests)
- [ ] `python smoke_test.py http://<tailscale-ip>:8081` passes (11 checks)
- [ ] No `.env`, API keys, tokens, real Tailscale addresses, databases or logs added
- [ ] `server.py` / `watchdog.py` still pass `py_compile`
- [ ] Documentation updated where behavior changed
- [ ] Backward compatibility with existing Android clients preserved
