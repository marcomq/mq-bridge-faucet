# mq-bridge-faucet

[faucet-stream](https://github.com/PawanSikawat/faucet-stream) connectors as an endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

```js
import { register } from "mq-bridge-faucet";

register();
```

Routes can then use `{ custom: { name: "faucet", config: { connector: "rest", config: {...} } } }`.
