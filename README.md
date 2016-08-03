# MT-Rewrite
Haskell project for rewriting MTSQL to SQL. MT-Rewrite is part of the [MTBase
project](https://github.com/mtbase/overview).

## Installation
Clone the project and then also pull the subproject on which mt-rewrite depends:

```
git clone https://github.com/lucasbraun/mt-rewrite.git
cd mt-rewrite
git submodule update --init
cd hssqlppp
git checkout master
git pull
cd ../multimap
git checkout master
git pull
cd ..
```
