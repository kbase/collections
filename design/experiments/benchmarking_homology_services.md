# Benchmarking homology services

Investigate the reliability and speed of the homology services - specifically the sketch
service and the assembly homology service - for the purposes of including them in a homology
matcher.

## Repos and services

https://github.com/kbaseapps/sketch_service
https://github.com/jgi-kbase/AssemblyHomologyService

https://homology.kbase.us/namespace
The sketch service is a dynamic service and so the URL is dynamic. The URL must be looked up
in the KBase Catalog Web Services UI.

## Experiments

### Test parallelization limits

**Summary:**
* The assembly homology service appears to be the bottleneck. 
* There are multiple failures in the logs where the homology service rejected the input sketch
  as invalid. It's not clear why that might be.
  * Incomplete upload?
  * Many mash instances competing for CPUs?

**Notes:**
* The test exercises the sketch service (and thus the homology service) with ever greater
  numbers of parallel requests.
  * As such, I suspect that earlier failures cause failures in the later tests, since the mash
    instances are probably still running.
    * Also, the sketch service will retry up to 7 times, so if the request fails due to a
      gateway timeout the sketch service could still be performing multiple retries.
    * In one test, the last response from the homology service was 3 minutes after the last
      test request to the sketch service, and the test ended after 100 seconds.
* The sketch service does no internal caching and so the same object will be downloaded
  multiple times if requested multiple times. The homology service also does no internal caching.
  The cache service is used for caching purposes but we instruct the sketch service to skip
  the cache lookup intentionally.
* The ID mapping service is also skipped as we don't use the `NCBI_Refseq` namespace, which is
  the only namespace that triggers ID lookups in the sketch service.
* Note also that while the homology services allows setting a minimum minhash distance, the sketch
  service does not.
* During the testing, NERSC shut down the homology service as it was using 120 cores. As
  such, Boris limited the service to 32 cores and we reduced the parallel connections in the test
  from a maximum of 1000 to 100.

**Results for Assemblies**
```
$ PYTHONPATH=. python design/experiments/sketch_service_benchmarking.py KB_AUTH_CI
performing parallel requests to https://ci.kbase.us:443/dynserv/d951457d6b973755217a65866f5d39b8544e4632.sketch-service
Performing 10 parallel requests
Starting request id 1 at 1685568548.663 sec
*snip*
Code: 524 message: 
100 errors out of 100 tasks in 100.29426193237305 sec

Reqs	Time	Errors	%
10	29.42	0	0.00
20	36.24	0	0.00
30	43.02	0	0.00
40	53.53	0	0.00
50	69.07	23	46.00
60	70.60	58	96.67
70	75.20	70	100.00
80	100.25	80	100.00
90	100.32	90	100.00
100	100.29	100	100.00

Errortypes
Requests: 10
	No errors
Requests: 20
	No errors
Requests: 30
	No errors
Requests: 40
	No errors
Requests: 50
	Unspecified AssemblyHomology failure	23
Requests: 60
	Unspecified AssemblyHomology failure	58
Requests: 70
	Code: 502 message: Bad Gateway	1
	Unspecified AssemblyHomology failure	69
Requests: 80
	Code: 524 message: 	80
Requests: 90
	Code: 502 message: Bad Gateway	1
	Code: 524 message: 	89
Requests: 100
	Code: 502 message: Bad Gateway	2
	Code: 524 message: 	98
```

**Results for Genomes**
```
Reqs	Time	Errors	%
10	37.81	0	0.00
20	49.78	0	0.00
30	54.04	0	0.00
40	67.77	0	0.00
50	81.25	0	0.00
60	94.05	25	41.67
70	98.51	58	82.86
80	100.25	71	88.75
90	100.27	90	100.00
100	100.64	100	100.00

Errortypes
Requests: 10
	No errors
Requests: 20
	No errors
Requests: 30
	No errors
Requests: 40
	No errors
Requests: 50
	No errors
Requests: 60
	Unspecified AssemblyHomology failure	25
Requests: 70
	Unspecified AssemblyHomology failure	58
Requests: 80
	Code: 524 message: 	39
	Unspecified AssemblyHomology failure	32
Requests: 90
	Code: 524 message: 	67
	Unspecified AssemblyHomology failure	23
Requests: 100
	("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)", InvalidChunkLength(got length b'', 0 bytes read))	1
	Code: 524 message: 	99
```

**Errors**

The AssemblyHomology failures seem to be caused by this output from `mash` in the service logs
when calling `mash info -H` on the input sketch:
```
minhash implementation stderr:
terminate called after throwing an instance of 'kj::ExceptionImpl'
  what():  src/capnp/message.c++:54: failed: expected segment != nullptr && segment->containsInterval(segment->getStartPtr(), segment->getStartPtr() + 1); Message did not contain a root pointer.
stack: 0x460609 0x463c5a 0x45b07c 0x42d155 0x41f3c4 0x43d5c2 0x426bb6 0x40a55e 0x7efe890c82e1 0x40a205
```

Why running multiple instances of `mash` would cause this error is not clear. It seems to occur
when the number of `mash` instances are greater than the number of available CPUs.

Also note that `mash`, at this point, hasn't had a release in 2.5 years and seems to be typical
academic abandonware.

It's also possible that somehow the input sketch is being corrupted during transfer from the
sketch service to the assembly homology service.

The 524 errors are Cloudflare timeout errors, so similar to Bad Gateway errors.

**Next**

* For demo purposes this is workable, but in production probably not
  * If 2 people start a match with 30 assemblies each the matches will probably fail
  * The maximum number of genomes in a match is 10000. If we serially run batches of 20 that will
    take 5-10 hours for a single match
* Could log md5s of input sketch in the sketch service and assembly homology service to verify
  upload is correct
* Assuming the problem is a bug in `mash` it seems highly unlikely that it'll ever be fixed
  unless we fork it and fix it ourselves.
* Making batch endpoints for the sketch service and assembly homology service may reduce the
  problem, but downloading hundreds or thousands of genomes in the sketch service in one call
  will probably cause a timeout.
  * We should test how long it takes to sketch 10k genomes against Refseq. If that's fast
    making a batch endpoint in the assembly homology service and calling it with an app that does
    the downloads might make sense.
    * In 1 hour 10M of 1B distances were calculated (single threaded) on the Perlmutter login node,
      meaning 100 CPU hours to do the full computation. That's not something we can support
      at all, service or app, presumably.
* We could investigate using another `mash` implementation that isn't abandonware.




