## MBrace.Core

This repository contains the foundations for the MBrace programming model and runtimes:

* [MBrace.Core](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Core): programming model essentials, cloud workflows, store primitives, libraries and client tools for authoring distributed code.
* [MBrace.Core.Tests](https://github.com/mbraceproject/MBrace.Core/tree/master/tests/MBrace.Core.Tests): general-purpose test suites available to runtime implementers.
* [MBrace.Runtime.Core](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Runtime.Core): foundations and common implementations for developing MBrace runtimes.
* [Sample runtime](https://github.com/mbraceproject/MBrace.Core/tree/master/samples/MBrace.SampleRuntime): Toy runtime implementation demonstrating fault-tolerant distrbuted computation using [Thespian](http://nessos.github.io/Thespian).

### Build Status

Head (branch master), Build & Unit tests
  * Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/3yaglw86q7vnja7w/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-core/branch/master)
  * Mac OS X/Mono 3.10 [![Build Status](https://travis-ci.org/mbraceproject/MBrace.Core.png?branch=master)](https://travis-ci.org/mbraceproject/MBrace.Core/branches)
