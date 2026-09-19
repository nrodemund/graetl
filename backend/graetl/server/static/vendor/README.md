# Vendored browser assets

Anything dropped here is served from `/vendor/...`.

The code editor looks for **`vendor/vs/loader.js`** (Monaco's AMD loader) first
and only falls back to the CDN — or, with no internet at all, to the small
built-in editor — when it is missing.

To fill it in:

```
graetl vendor-monaco                       # downloads the npm tarball
graetl vendor-monaco --from node_modules/monaco-editor   # offline, from npm install
```

`vendor/vs/` is deliberately not committed: it is ~5 MB of third-party build
output under the MIT licence.
