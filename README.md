# mt-rewrite
Haskell project for rewriting SQL queries in Multi-Tenancy (MT) semantics.

## Installation
Clone the project and then also pull the hssqlppp subproject on which mt-rewrite depends:

```bash
git clone https://github.com/lucasbraun/mt-rewrite.git
cd mt-rewrite
git submodule update --init
cd hssqlppp
git checkout master
git pull
```
