# kvstore

To create a development environment:
```bash
vagrant up
vagrant ssh
```

To build the project:
```
cd /vagrant
mkdir /tmp/build  # Can't mmap files in shared vagrant directory...
cmake /vagrant
make
```

To run tests from the build directory:
```bash
./run_tests
```
