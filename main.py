# Imports
from zarr_conv import create_zarr_array, save_image_to_zarr
from save_webp import run
from dask.distributed import Client, LocalCluster
import argparse


def main(lossy_path, zarr_path, zoomify_dir, img_type, patch_size=32768):
    # Initialising the Dask cluster
    client = Client("tcp://127.0.0.1:43979")

    # Using Dask to convert image to zarr arrays. This will need lossy_path, 
    # zarr_path and patch_size as input
    arr_path = save_image_to_zarr(lossy_path, zarr_path, client, patch_size=32768)
    print(f"Zarr array saved at {arr_path}")

    # Using dask to convert zarr arrays to image tiles in webp format. 
    # This will need zarr_path and zoomify_dir and img_type as input.
    save_dir = run(arr_path, zoomify_dir, client)
    print(f"Image tiles saved at {save_dir}")

    # Using dask to convert image tiles in webp format to a zipped file. 
    # This will need zoomify_dir as input. Output is not needed as we will use the zoomify_dir for zip.
    # TODO: Fix this file to be in a better format

# main(lossy_path, zarr_path, zoomify_dir, img_type, patch_size=32768)

if __name__ == "__main__":
    from constants import lossy_path, zoomify_dir, zarr_path

    parser = argparse.ArgumentParser(description="Process some paths and image type.")
    parser.add_argument(
        "--lossy_path",
        type=str,
        required=False,
        help="Path to the lossy image",
        default=lossy_path,
    )
    parser.add_argument(
        "--zarr_path",
        type=str,
        required=False,
        help="Path to save the zarr array",
        default=zarr_path,
    )
    parser.add_argument(
        "--zoomify_dir",
        type=str,
        required=False,
        help="Directory to save the zoomified images",
        default=zoomify_dir,
    )
    parser.add_argument(
        "--img_type", type=str, required=False, help="Type of the image", default="webp"
    )

    args = parser.parse_args()

    main(
        args.lossy_path,
        args.zarr_path,
        args.zoomify_dir,
        args.img_type,
        patch_size=32768,
    )
