import zarr
import numpy as np
from PIL import Image
from dask import delayed
from distributed import Client, LocalCluster
from tqdm import trange
from time import perf_counter
import glymur
from numcodecs import blosc
import sys

# Set threading options for performance
blosc.set_nthreads(8)
blosc.use_threads = True
glymur.set_option("lib.num_threads", 2)


# Initialize Dask cluster
def initialize_dask_cluster():
    cluster = LocalCluster(
        dashboard_address=":8877", n_workers=1, processes=True, memory_limit="16GB"
    )
    client = Client(cluster)
    return client


# Open JP2 image using glymur
def open_image(img_path):
    try:
        print(f"Opening image: {img_path}")
        img_data = glymur.Jp2k(img_path)
        img = img_data[:]
        return img_data
    except Exception as e:
        print(f"Error opening image: {e}")
        return None


# Create Zarr array to store the image


def create_zarr_array(img_path, arr_path, chunk_size=(2048, 2048, 3)):
    img = open_image(img_path)
    store = zarr.DirectoryStore(
        arr_path
    )  # arr_path is where the zarr file will be saved
    img_zarr = zarr.zeros(
        shape=img.shape, chunks=chunk_size, dtype="uint8", store=store, overwrite=True
    )
    return img_zarr, store


# Delayed function to save a tile to Zarr
@delayed
def save_tile_zarr(i, j, img, zarr_store, patch_size=32768):
    try:
        di = min(img.shape[0], i + patch_size)
        dj = min(img.shape[1], j + patch_size)
        tile = img[i:di, j:dj, :]
        zarr_store[i:di, j:dj, :] = tile
        return True
    except Exception as e:
        print(f"Error saving tile: {e}")
        return False


# Main function to save image tiles to Zarr
def save_image_to_zarr(img_path, arr_path, client, patch_size=32768):
    # Initialize Dask cluster
    # client = initialize_dask_cluster()
    # print(f"Client started")

    # Open image and create Zarr array
    img = open_image(img_path)
    img_zarr, store = create_zarr_array(img_path, arr_path)

    # Generate tasks to save tiles
    task_index = []
    for i in trange(0, img.shape[0], patch_size):
        for j in range(0, img.shape[1], patch_size):
            task = save_tile_zarr(i, j, img, img_zarr, patch_size)
            task_index.append(task)

    print(f"Total Tasks: {len(task_index)}")

    # Execute tasks using Dask
    start = perf_counter()
    futures = client.compute(task_index, sync=True)
    results = client.gather(futures)
    end = perf_counter()

    print(f"Time taken to save {len(task_index)} tiles: {end - start} seconds")
    return zarr.open(arr_path, mode="r")


# Main entry point
if __name__ == "__main__":
    # Check if the script is run with correct arguments
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <image_path> <zarr_output_path>")
        sys.exit(1)

    # Get image and Zarr paths from command line arguments
    img_path = sys.argv[1]
    arr_path = sys.argv[2]

    # Run the tiling process
    save_image_to_zarr(img_path, arr_path)
