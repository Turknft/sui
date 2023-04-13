window.BENCHMARK_DATA = {
  "lastUpdate": 1681419268479,
  "repoUrl": "https://github.com/MystenLabs/sui",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "106119108+gegaowp@users.noreply.github.com",
            "name": "Ge Gao",
            "username": "gegaowp"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7d6070e6efba4229a8b838f8429840d7783c55da",
          "message": "sui ns rpc: resolve between address and name (#10684)\n\nThe related Move change is: \r\nhttps://github.com/MystenLabs/sui/compare/ds/archive?expand=1\r\n\r\n## Test Plan \r\n\r\nTest locally with Move contract ^^\r\n1. start Sui network, publish module and make move calls to add new\r\npairs\r\n```\r\ntarget/debug/sui start\r\n\r\n// under sui/sui_programmability/examples/archive\r\nsui client publish --gas-budget 100000000\r\n\r\nsui client call --package 0xf12ab3bf6c1b53add39529bedd0ebb1b083a3dc7e4736ff35bbeccf2be80f80f --module archive --function add_record --gas-budget 100000000 --args 0xd3db0bd1b6847412b1aa0e4f0ff5c96eceba1a8cc0f30df5946f38102b8da3fa 0x6 0x54f349899f6b521547bcfcb2605c65f307934b4f32c256fd3e2d4df6feecb870 \"ccc\"\r\n```\r\n2. stop Sui network, override FN config file with the fetched resolver\r\nobject ID by adding this line to ~/.sui/sui_config/fullnode.yaml\r\n```\r\nname-service-resolver-object-id: <archive_object_id>\r\n```\r\n\r\n3. restart Sui network without FN; start FN based on config with fetched\r\nresolved object ID\r\n```\r\ntarget/debug/sui start --no-full-node\r\n\r\ncargo run --bin sui-node -- --config-path  /Users/gegao/.sui/sui_config/fullnode.yaml\r\n\r\n```\r\n5. test on RPC\r\n```\r\ncurl --location --request POST http://127.0.0.1:9000 \\\r\n--header 'Content-Type: application/json' \\\r\n--data-raw '{\r\n    \"jsonrpc\": \"2.0\",\r\n    \"id\": 1,\r\n    \"method\": \"suix_resolveNameServiceAddress\",\r\n    \"params\": [\"ccc\"]\r\n}'\r\n{\"jsonrpc\":\"2.0\",\"result\":\"0x54f349899f6b521547bcfcb2605c65f307934b4f32c256fd3e2d4df6feecb870\",\"id\":1}%                                                                 \r\n\r\ncurl --location --request POST http://127.0.0.1:9000 \\\r\n--header 'Content-Type: application/json' \\\r\n--data-raw '{\r\n    \"jsonrpc\": \"2.0\",\r\n    \"id\": 1,\r\n    \"method\": \"suix_resolveNameServiceNames\",\r\n    \"params\": [\"0x54f349899f6b521547bcfcb2605c65f307934b4f32c256fd3e2d4df6feecb870\"]\r\n}'\r\n{\"jsonrpc\":\"2.0\",\"result\":[\"ccc\"],\"id\":1}%\r\n\r\n```",
          "timestamp": "2023-04-13T20:41:35Z",
          "tree_id": "daa68e1ef6b2328f8ab116f992719bdda16b3d48",
          "url": "https://github.com/MystenLabs/sui/commit/7d6070e6efba4229a8b838f8429840d7783c55da"
        },
        "date": 1681419187713,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist_checkpoint",
            "value": 181040025,
            "range": "± 12379118",
            "unit": "ns/iter"
          },
          {
            "name": "get_checkpoint",
            "value": 687673,
            "range": "± 63145",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "81660174+mwtian@users.noreply.github.com",
            "name": "mwtian",
            "username": "mwtian"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bf757cc486cb5ff33e3427b0e1b5de1471263540",
          "message": "[TransactionManager] refactor and fix memory usage issues (#10829)\n\n## Description \r\n\r\n1. Fix a leak where lock_waiters map inserts an entry with empty\r\nLockQueue, that may not get removed.\r\n2. Resize HashMaps in TM as load changes.\r\n3. Only notify TM once after a transaction commits, with both tx digest\r\nand output keys.\r\n4. Extract common logic to update lock queue and ready transactions\r\nafter a transaction and its output objects commit.\r\n\r\n## Test Plan \r\n\r\nunit tests\r\n\r\n---\r\nIf your changes are not user-facing and not a breaking change, you can\r\nskip the following section. Otherwise, please indicate what changed, and\r\nthen add to the Release Notes section as highlighted during the release\r\nprocess.\r\n\r\n### Type of Change (Check all that apply)\r\n\r\n- [ ] user-visible impact\r\n- [ ] breaking change for a client SDKs\r\n- [ ] breaking change for FNs (FN binary must upgrade)\r\n- [ ] breaking change for validators or node operators (must upgrade\r\nbinaries)\r\n- [ ] breaking change for on-chain data layout\r\n- [ ] necessitate either a data wipe or data migration\r\n\r\n### Release notes",
          "timestamp": "2023-04-13T20:43:03Z",
          "tree_id": "01faabd751ba32d7f312aea9cdb7aab09b97f133",
          "url": "https://github.com/MystenLabs/sui/commit/bf757cc486cb5ff33e3427b0e1b5de1471263540"
        },
        "date": 1681419266126,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist_checkpoint",
            "value": 180936974,
            "range": "± 12790856",
            "unit": "ns/iter"
          },
          {
            "name": "get_checkpoint",
            "value": 541838,
            "range": "± 22867",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}