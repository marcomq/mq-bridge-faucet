# mq-bridge-faucet

[faucet-stream](https://github.com/PawanSikawat/faucet-stream) connectors as an endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

```python
import mq_bridge_faucet

mq_bridge_faucet.register()
```

Routes can then use `{"custom": {"name": "faucet", "config": {"connector": "rest", "config": {...}}}}`.
