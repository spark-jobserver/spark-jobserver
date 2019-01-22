<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [The included Akka Stack](#the-included-akka-stack)
- [Useful Traits and Utilities](#useful-traits-and-utilities)
- [For more info](#for-more-info)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

akka_app defines a standard stack for Akka applications, as well as traits and utilities for Akka Actors.

## The included Akka Stack

* Akka 2.2.4 (including remote + testkit)
* JodaTime
* Yammer Metrics for stats and instrumentation
* spray-json for JSON serialization
* spray for embedded web server, with some common routes like /metricz and /statusz

## Useful Traits and Utilities

* ActorStack - a base trait for enabling stackable Akka Actor traits
* Slf4jLogging - directly log to Slf4j + add akkaSource MDC context for the actor's path
* ActorMetrics - instrument receive handler duration and invocation frequency
* CommonRoutes - /metricz, /statusz

## For more info

Please see the following presentation for more info on how we use stackable traits and Akka at Ooyala:

http://www.slideshare.net/EvanChan2/akka-inproductionpnw-scala2013
