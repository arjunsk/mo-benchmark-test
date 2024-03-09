
|                      | Insert duration 10_000 | Query QPS 1_000 |
| Without Index        | 7.2sec          | 31 |
| With Secondary Index | 48sec           | BUG |
| With Master Index    | 14sec           | 3.4 |