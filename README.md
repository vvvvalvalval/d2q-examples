# d2q-examples

Examples of using [d2q](https://github.com/vvvvalvalval/d2q) in combination with various data storages / backends.

**Domain:** A Blog engine with User, Post and Comment entities. The d2q API schema is described [**here**](./src/d2q_examples/api_schema.clj).

**Implementations:**

* [DataScript backend](./src/d2q_examples/datascript.clj). Although it uses [DataScript](https://github.com/tonsky/datascript),
 it could easily be adapted to [Datomic](https://docs.datomic.com/on-prem/query.html). Runs in-memory, no external setup needed.
* [SQL backend](./src/d2q_examples/sql.clj). Uses the [H2](http://www.h2database.com/html/main.html) database via [JDBC](https://github.com/clojure/java.jdbc);
 it could easily adapted to at least PostgreSQL and SQLite (some SQL engines like MySQL make this more difficult as far as I can tell, 
 see [this discussion on StackOverflow](https://stackoverflow.com/questions/178479/preparedstatement-in-clause-alternatives/10240302#10240302)).
 Runs in-memory, no external setup needed.
 
## License

Copyright © 2018 Valentin Waeselynck

Distributed under the MIT license.
