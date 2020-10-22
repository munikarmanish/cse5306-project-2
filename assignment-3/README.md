# Distributed locking

In this project, we implemented the following two locking algorithms:

* Centralized coordinated locking
* Distributed locking

## Project members

* Hamza Reza Pavel
* Manish Munikar

## Requirements

* Python >= 3.7

## How to run

    $ python3 central.py
    $ python3 distributed.py

In each test, we create a counter file `data` with an initial value of "0".
Then we start 5 independent member processes that will increment the value in
the `data` file 10 times each. Therefore, after all operations are
successfully completed, the `data` file should have the value "50".
