Databroker Extensions
=====================

.. image:: https://img.shields.io/travis/elParcero/databroker_extensions.svg
        :target: https://travis-ci.org/elParcero/databroker_extensions

.. image:: https://img.shields.io/pypi/v/databroker_extensions.svg
        :target: https://pypi.python.org/pypi/databroker_extensions


Extensions for databroker software.

* Free software: 3-clause BSD license
* Documentation: (COMING SOON!) https://elParcero.github.io/databroker_extensions.

Installing
----------
```
git clone https://github.com/elParcero/databroker_extensions.git
```

Example 1
---------
If the user just wants to find file usage, the following block of code will work and is an example using CHX beamline.
    broker_extensions import *

    db = Broker.named("chx")
    db.reg.register_handler("AD_EIGER", EigerHandler )
    db.reg.register_handler("AD_EIGER2", EigerHandler)
    db.reg.register_handler("AD_EIGER_SLICE", EigerHandler)
    db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)

    since = "2017-01-01"
    until = "2018-12-31"
    file_size = file_sizes(db, since, until, plan="count")

Example 2
---------
If the user just wants to find file usage for specific plan, the following block of code will work and is an example using CHX beamline.

Example 3
---------
If the user just wants to find file usage for specific plan and detectors, the following block of code will work and is an example using CHX beamline.

Features
--------

* Extracts file usage 
* Extracts file last modified
* Extracts file last accessed
