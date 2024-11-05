# Imports
from constants import lossy_path, zoomify_dir, img_type, zarr_path
from zarr_conv import create_zarr_array, save_image_to_zarr
from save_webp import run
from dask.distributed import Client, LocalCluster

# Defining the main function

lossy_path = lossy_path
zarr_path = zarr_path
zoomify_dir = zoomify_dir
img_type = img_type


# Initialising the Dask cluster
# def initialize_dask_cluster():
#     cluster = LocalCluster(dashboard_address=':8877', n_workers=1, processes=True, memory_limit='16GB')
#     client = Client(cluster)
#     return client


def main(lossy_path, zarr_path, zoomify_dir, img_type, patch_size=32768):
    ##Initialising the Dask cluster
    client = Client("tcp://127.0.0.1:43979")

    ##Using Dask to convert image to zarr arrays. This will need lossy_path, zarr_path and patch_size as input
    arr_path = save_image_to_zarr(lossy_path, zarr_path, client, patch_size=32768)
    print(f"Zarr array saved at {arr_path}")

    ##Using dask to convert zarr arrays to image tiles in webp format. This will need zarr_path and zoomify_dir and img_type as input.
    save_dir = run(arr_path, zoomify_dir, client)
    print(f"Image tiles saved at {save_dir}")

    ##Using dask to convert image tiles in webp format to a zipped file. This will need zoomify_dir as input. Output is not needed as we will use the zoomify_dir for zip.


main(lossy_path, zarr_path, zoomify_dir, img_type, patch_size=32768)
