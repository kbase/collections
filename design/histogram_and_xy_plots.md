# Histogram and XY plots

* Add endpoints to support:
  * Getting a histogram of a single numerical column's values
    * Specify columns and number of bins
    * Note that to create a histogram we need to pull all the data into memory at once
      for normal algorithms, although there are
      [heuristic online algorithms](https://stackoverflow.com/questions/6385700/calculating-a-histogram-on-a-streaming-data-online-histogram-calculation)
      * Unless the user supplies the bin locations, but that's not very user friendly
  * Getting data for an x y scatter plot from 2 numerical columns
    * Specify columns
    * No binning for now, although hexbins could be a future improvement and has
      an [online algorithm](https://github.com/coryfoo/hexbins/blob/master/hexbin/binner.py)
  * Both endpoints should support matches, selections and filters
  * Don't worry about performance for now
    * Pulling 2 columns of GTDB data (~317k entries) takes ~3s on docker02 when
      connecting to CI Arango (see appendix 1)
    * Indexing could help
    * Caching could help
    * Precalculated data could help for popular plots
      * Although filters would make this not very useful
* For now we won't worry about enum data because we don't have any columns marked as enums
* For now users cannot select columns for plots
  * But that's something we may do in the future
* We'd like to be able to plot millions of points, but at this stage being able to plot
  GTDB (~300-400k points) would be great, but maybe not feasible
* No need to mark matches / selections in plots, just filter by them
* Ideally filters can also be applied by selecting on the graph itself
  * This is somewhat negotiable. Maybe 2nd iteration
* Re Cody's mockups:
  * For now, only support 3 panels:
    * Filtered
      * Allow matched and selected as filters
    * Matched
    * Selected
    * E.g. the matched & selected panes cannot have separate sets of filters applied.

## Appendix 1 - GTDB 2 column timing

Run on docker02

```
root@0e7ba63a5fc1:/# ipython
Python 3.11.6 (main, Oct 12 2023, 10:04:56) [GCC 12.2.0]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.16.1 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import aioarango, os, time

In [2]: arango_pwd = os.environ.get("APWD")

In [3]: cli = aioarango.ArangoClient(hosts='http://10.58.1.211:8531')

In [4]: db = await cli.db("collections_dev", username="collections_dev", passwor
   ...: d=arango_pwd)

In [5]: aql = """
   ...:     FOR d IN @@collection
   ...:         FILTER d.coll == @coll
   ...:         FILTER d.load_ver == @lv
   ...:         RETURN KEEP(d, [@field1, @field2])
   ...:     """

In [6]: bind_vars = {
   ...:     "@collection": "kbcoll_genome_attribs",
   ...:     "coll": "GTDB",
   ...:     "lv": "r207.kbase.1",
   ...:     "field1": "checkm_completeness",
   ...:     "field2": "checkm_contamination",
   ...: }

In [7]: async def xy():
   ...:     ret = {}
   ...:     print(time.time())
   ...:     async for d in await db.aql.execute(aql, bind_vars=bind_vars):
   ...:         ret[d["checkm_completeness"]] = d["checkm_contamination"]
   ...:     print(time.time())
   ...:     return ret
   ...: 

In [8]: ret = await xy()
1697583971.1108532
1697583973.797272

In [9]: ret = await xy()
1697584070.5995398
1697584073.423372
```