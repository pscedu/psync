## psync - the parallel file transfer tool

psync is an rsync-like clone that uses multiple transfer streams in
parallel for transferring file data, designed to achieve higher
performance when transferring between parallel file system backends.

Most rsync features are not implemented yet; however the core transfer
mechanism has already provided much utility in our environments.

More usage details are available in the psync(1) manual page included
in the distribution.

### Installation

Requires GNU make >= 3.81.

Grab PFL:

    $ git clone https://www.github.com/pscedu/pfl psc-projects
    $ cd psc-projects

Grab psync:

    $ git clone https://www.github.com/pscedu/psync

Build:

    $ cd psync && make build

Install:

    $ sudo make install

### Copyright

Pittsburgh Supercomputing Center
http://www.psc.edu/
