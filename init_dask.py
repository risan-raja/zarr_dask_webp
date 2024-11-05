import logging
from dask.distributed import Client, LocalCluster
import time


def initialize_dask_cluster():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("distributed")
    logger.setLevel(logging.DEBUG)  # For distributed logging
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    print("Starting Dask LocalCluster...")
    cluster = LocalCluster(
        ip="0.0.0.0",
        dashboard_address="0.0.0.0:5001",
        n_workers=2,
        processes=True,
        memory_limit="16GB",
    )
    client = Client(cluster)
    print("Dask cluster initialized:")
    print(client)

    # Run a sample computation to trigger logs
    import dask.array as da

    x = da.random.random((10000, 10000), chunks=(1000, 1000))
    x.sum().compute()  # Trigger a computation to generate activity and logs

    # Keep the cluster running
    try:
        while True:
            time.sleep(10)  # Keep the program alive
    except KeyboardInterrupt:
        print("Shutting down Dask cluster...")
        client.close()
        cluster.close()


if __name__ == "__main__":
    initialize_dask_cluster()
