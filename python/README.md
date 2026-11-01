# mq-bridge-faucet

[faucet-stream](https://github.com/faucet-hq/faucet-stream) connectors as an endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

> Not an official faucet-stream project, and not affiliated with its authors.
> The plugin adapts faucet-stream's connectors to mq-bridge, which may behave
> differently from faucet-stream itself; report problems seen through the
> plugin [here](https://github.com/marcomq/mq-bridge-faucet/issues) unless they
> also occur with faucet-stream alone.

```python
import mq_bridge_faucet

mq_bridge_faucet.register()
```

Routes can then use `{"custom": {"name": "faucet", "config": {"connector": "rest", "config": {...}}}}`.
