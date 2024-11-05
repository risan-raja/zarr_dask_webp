from init_dask import initialize_dask_cluster

if __name__ == "__main__":
    client = initialize_dask_cluster()

    # Example computation to generate logs
    import dask.array as da

    x = da.random.random((10000, 10000), chunks=(1000, 1000))
    x.sum().compute()  # Trigger a computation to generate activity and logs
