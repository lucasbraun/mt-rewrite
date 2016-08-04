# MT-Rewrite
Haskell project for rewriting MTSQL to SQL. MT-Rewrite is part of the [MTBase
project](https://github.com/mtbase/overview). In order to install MT-Rewrte,
you need to install `stack` first.

## Installation
Clone the project and then also pull the subproject on which mt-rewrite depends. After that, build the project with stack:

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
stack build
```
