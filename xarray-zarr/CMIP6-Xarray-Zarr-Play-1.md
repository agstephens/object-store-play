# Experimenting with writing CMIP6 data to Zarr - June 2020

## Objective

To read in a CMIP6 Dataset, comprised of one a time series for variable across many NetCDF files, 
using Xarray and to write the same data to a Zarr file-based backend (on JASMIN).

The Dataset is 100s of GBs in size - so it must be _chunked_ in order to work.

## The script

Here is an example script for testing out the conversion of a CMIP6 Dataset to a Zarr file (on-disk). 
The script includes setting some environment variables to tell Numpy and Dask to _not_ parallelise its processing.

```
import xarray as xr
import os


def _setup_env():
    """
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
"""
    env = os.environ
    env['OMP_NUM_THREADS'] = '1'
    env['MKL_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'

_setup_env()


def main():
    dr = '/badc/cmip6/data/CMIP6/HighResMIP/MOHC/HadGEM3-GC31-HH/control-1950/r1i1p1f1/day/ta/gn/v20180927'
    print(f'[INFO] Working on: {dr}')

    ds = xr.open_mfdataset(f'{dr}/*.nc') # , parallel=False)

    chunk_rule = {'time': 4}
    chunked_ds = ds.chunk(chunk_rule)

    print(f'[INFO] Chunk rule: {chunk_rule}')

    OUTPUT_DIR = '/gws/nopw/j04/cedaproc/astephen/ag-zarr-test'
    output_path = f'{OUTPUT_DIR}/test.zarr'

    chunked_ds.to_zarr(output_path)
    print(f'[INFO] Wrote: {output_path}')


if __name__ == '__main__':

    main()
    
```

I assumed that chunking the data set into chunks of 4 time steps will keep the memory footprint low. I would expect the code to be doing:

 - while chunks still to process:
   - read chunk
   - write chunk to zarr

In theory, that would only ever need to keep the size of 1 chunk in memory. Each chunk should be less than 100Mbytes. Here is what happens when I run the code (I have ommitted non-essential output):

```
$ python quick.py

[INFO] Working on: /badc/cmip6/data/CMIP6/HighResMIP/MOHC/HadGEM3-GC31-HH/control-1950/r1i1p1f1/day/ta/gn/v20180927
[INFO] Chunk rule: {'time': 4}


Traceback (most recent call last):
  File "quick.py", line 39, in <module>
    main()
  File "quick.py", line 33, in main
    chunked_ds.to_zarr(output_path)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/core/dataset.py", line 1634, in to_zarr
    append_dim=append_dim,
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/backends/api.py", line 1344, in to_zarr
    writes = writer.sync(compute=compute)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/backends/common.py", line 204, in sync
    regions=self.regions,
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/array/core.py", line 945, in store
    result.compute(**kwargs)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/base.py", line 166, in compute
    (result,) = compute(self, traverse=False, **kwargs)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/base.py", line 444, in compute
    results = schedule(dsk, keys, **kwargs)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/threaded.py", line 84, in get
    **kwargs
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/local.py", line 486, in get_async
    raise_exception(exc, tb)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/local.py", line 316, in reraise
    raise exc
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/local.py", line 222, in execute_task
    result = _execute_task(task, data)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/core.py", line 121, in _execute_task
    return func(*(_execute_task(a, cache) for a in args))
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/dask/array/core.py", line 100, in getter
    c = np.asarray(c)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/numpy/core/_asarray.py", line 85, in asarray
    return array(a, dtype, copy=False, order=order)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/core/indexing.py", line 491, in __array__
    return np.asarray(self.array, dtype=dtype)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/numpy/core/_asarray.py", line 85, in asarray
    return array(a, dtype, copy=False, order=order)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/core/indexing.py", line 653, in __array__
    return np.asarray(self.array, dtype=dtype)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/numpy/core/_asarray.py", line 85, in asarray
    return array(a, dtype, copy=False, order=order)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/core/indexing.py", line 557, in __array__
    return np.asarray(array[self.key], dtype=None)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/numpy/core/_asarray.py", line 85, in asarray
    return array(a, dtype, copy=False, order=order)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/coding/variables.py", line 72, in __array__
    return self.func(self.array)
  File "/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.6.14/envs/jaspy3.7-m3-4.6.14-r20200606/lib/python3.7/site-packages/xarray/coding/variables.py", line 142, in _apply_mask
    return np.where(condition, decoded_fill_value, data)
  File "<__array_function__ internals>", line 6, in where
  
MemoryError: Unable to allocate 4.22 GiB for an array with shape (180, 8, 768, 1024) and data type float32
```

The overall volume of the dataset (summing the file sizes) is:

~320GB

When I looked into this, the total shape of the variable `ta` across the whole Dataset was: 

```
(Pdb) ds.ta.shape
(36360, 8, 768, 1024)
(Pdb) ds.ta.size/2**30
213.046875 # GB
```

This doesn't agree with the internal view Xarray has of the Dataset:

```
(Pdb) ds.ta.nbytes/(2.**30)
852.1875 # GB - 4 times as big - strange!
(Pdb) ds.ta.shape
(36360, 8, 768, 1024)
```

But we can see how Xarray got there:

```
(Pdb) size = 36360*8*768*1024*4
(Pdb) size
915029360640
(Pdb) size / 2**30
852.1875
```

The size for a single file was:
 - 4 (bytes per value) * 180 (time) * 8 (level) * 768 (lat) * 1024 (lon)
  = 4.22 GiB

And the memory error is: "Unable to allocate 4.22 GiB for an array with shape (180, 8, 768, 1024)".
 
So, it is trying to read the full file into memory and failing. 

Why is it trying to do that?

Is it related to DA versus DS:

```
(Pdb) chunkded_ds.ta.unify_chunks()
(Pdb) chunked_ds.ta.chunks
((4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4), (8,), (768,), (1024,))
(Pdb) chunked_ds.chunks
*** ValueError: Object has inconsistent chunks along dimension time. This can be fixed by calling unify_chunks().

```

Success! It works now, after unifying chunks, with memory footprint as follows:

```
[INFO] Wrote: /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr
Filename: quick.py

Line #    Mem usage    Increment   Line Contents
================================================
    26       99 MiB       99 MiB   @profile(precision=0)
    27                             def main():
    28       99 MiB        0 MiB       dr = '/badc/cmip6/data/CMIP6/HighResMIP/MOHC/HadGEM3-GC31-HH/control-1950/r1i1p1f1/day/ta/gn/v20180927'
    29       99 MiB        0 MiB       src = dr
    30       99 MiB        0 MiB       src = glob.glob(f'{dr}/*.nc')[:N]
    31       99 MiB        0 MiB       print(f'[INFO] Working on: {dr}')
    32
    33       99 MiB        0 MiB       start = time.time()
    34       99 MiB        0 MiB       chunk_rule = {'time': 4}
    35
    36      296 MiB      196 MiB       chunked_ds = xr.open_mfdataset(src, chunks=chunk_rule) # , parallel=False)
    37      296 MiB        0 MiB       print(f'[INFO] Opened Dataset, after {time.time() - start:.2f} seconds')
    38
    39                             #    import pdb; pdb.set_trace()
    40                             #    assert(chunked_ds.chunks['time'] == 4)
    41
    42      296 MiB        0 MiB       print(f'[INFO] Chunk rule: {chunk_rule}')
    43
    44      296 MiB        0 MiB       OUTPUT_DIR = '/gws/nopw/j04/cedaproc/astephen/ag-zarr-test'
    45      296 MiB        0 MiB       output_path = f'{OUTPUT_DIR}/test.zarr'
    46
    47      296 MiB        0 MiB       var_id = dr.split('/')[-3]
    48      296 MiB        0 MiB       chunked_ds[var_id].unify_chunks()
    49
    50     3500 MiB     3204 MiB       chunked_ds.to_zarr(output_path)
    51     3500 MiB        0 MiB       print(f'[INFO] Wrote: {output_path}')
```

But look at the volume of the Zarr file:

```
$ pan_du /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr/
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//lat: 4 files, 3 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//lon: 4 files, 3 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//plev: 4 files, 2 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//time: 5 files, 4 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//time_bnds: 5 files, 13 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//lon_bnds: 189 files, 5023 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//lat_bnds: 192 files, 3876 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr//ta: 8506 files, 429258345 KiB
dir /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr/: 8912 files, 429267272 KiB
```

It is 430Gbytes!!! - it has grown by 30%!

Let's look at the data:

```
$ ncdump -v ta /badc/cmip6/data/CMIP6/HighResMIP/MOHC/HadGEM3-GC31-HH/control-1950/r1i1p1f1/day/ta/gn/files/d20180927/ta_day_HadGEM3-GC31-HH_control-1950_r1i1p1f1_gn_19500101-19500630.nc | grep -A 3 -P "(data:|_FillValue)"
                ta:_FillValue = 1.e+20f ;
                ta:history = "2018-09-18T15:12:00Z altered by CMOR: Inverted axis: plev." ;

// global attributes:
--
data:

 ta =
  _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,

```

The first set of values are missing, so should be 1e+20f. Let's look at the first chunk in the Zarr file:

```
$ od -t f4 -N 16 /gws/nopw/j04/cedaproc/astephen/ag-zarr-test/test.zarr/ta/0.0.0.0
0000000   1.8925922e-36   2.4074124e-35     3.67342e-40   4.3115967e-37

```

This suggests that a compression method is being applied. It is using:

```
    "compressor": {
        "blocksize": 0,
        "clevel": 5,
        "cname": "lz4",
        "id": "blosc",
        "shuffle": 1
    },
```

## What next?

Some thoughts:

1) What if this is set to True?
   xr.open_mfdataset('my/files/*.nc', parallel=True)
   
2) Why is Zarr bigger than NetCDF version?

3) What if we change the environment variables?

4) Is Caringo quicker/slower than to disk? By what factor?

5) Should we set all these ENV VARS (based on Met Office numpy/dask advice)?
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

6) Should we set this dask option?
dask.config.set(scheduler='synchronous')
So that probably explains the issue. However, why is it not smaller than the NetCDF version.

