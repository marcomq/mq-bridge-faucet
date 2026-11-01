# mq-bridge-faucet

[faucet-stream](https://github.com/faucet-hq/faucet-stream) connectors as an endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

> Not an official faucet-stream project, and not affiliated with its authors.
> The plugin adapts faucet-stream's connectors to mq-bridge, which may behave
> differently from faucet-stream itself; report problems seen through the
> plugin [here](https://github.com/marcomq/mq-bridge-faucet/issues) unless they
> also occur with faucet-stream alone.

```js
import { register } from "mq-bridge-faucet";

register();
```

Routes can then use `{ custom: { name: "faucet", config: { connector: "rest", config: {...} } } }`.

## Native library

The plugin library is too large to ship inside the npm package, so
`register()` downloads the archive for your platform from this version's
[GitHub release](https://github.com/marcomq/mq-bridge-faucet/releases) on first
use, checks it against the sha256 pinned in the package, and caches it
(`~/.cache`, `~/Library/Caches` or `%LOCALAPPDATA%`, under `mq-bridge-faucet/`).
A conda or Homebrew install is used instead if present.

```sh
npx mq-bridge-faucet      # optional: download now, e.g. in a Docker build
```

Environment variables:

- `MQ_BRIDGE_FAUCET_LIBRARY`: path of a library to load instead (not checksummed).
- `MQ_BRIDGE_FAUCET_NO_DOWNLOAD`: never download; fail if no library is found.
- `MQ_BRIDGE_FAUCET_DOWNLOAD_URL`: fetch the archives from a mirror (the checksum still applies).
- `MQ_BRIDGE_FAUCET_CACHE`: cache directory.
