import os
import shutil
import sys
import numpy as np
from PIL import Image
from dask import delayed

# Define constants for the saving directory and stride
SAVE_DIR = "159_994"
if os.path.exists(SAVE_DIR):
    print("Removing existing directory of tiles\n")
    shutil.rmtree(SAVE_DIR)
    print("Creating directory of tiles\n")
    os.makedirs(SAVE_DIR)
else:
    print("Creating directory of tiles")
    os.makedirs(SAVE_DIR, exist_ok=True)

# Calculate max_level based on the Zarr shape
# max_level = np.ceil(np.log2(min(img_zarr.shape[:2]) / 2048))


def save_tile_img(i, j, stride=2048, store=None, save_dir=SAVE_DIR):
    max_level = np.ceil(np.log2(min(store.shape[:2]) / 2048))
    z = int(np.ceil(np.log2(stride / 2048)))
    level = max_level - z
    di = min(store.shape[0], i + stride)
    dj = min(store.shape[1], j + stride)

    if z > 0:
        tile = store[i:di, j:dj, :][::z, ::z]
    else:
        tile = store[i:di, j:dj, :]

    quality = 30
    img = Image.fromarray(np.ascontiguousarray(tile))
    try:
        img.save(
            f"{save_dir}/{int(level)}-{i // stride}-{j // stride}.jpg",
            "JPEG",
            quality=quality,
        )
    except Exception as e:
        print(i, j, di, dj, stride, tile.shape)
        sys.exit(1)
    return True


def run(img_zarr, save_dir, client):
    max_level = np.ceil(np.log2(min(img_zarr.shape[:2])/2048))
    task_index = []
    patch_size = 2048
    count = 0

    while True:
        if patch_size > max(img_zarr.shape[:2]):
            break

        for i in range(0, img_zarr.shape[0], patch_size):
            for j in range(0, img_zarr.shape[1], patch_size):
                z = int(np.ceil(np.log2(patch_size / 2048)))
                level = max_level - z  # Calculate the level for the current tile

                if level == 0 or level == 1:
                    # Run without Dask for levels 0 and 1
                    save_tile_img(
                        i, j, stride=patch_size, store=img_zarr, save_dir=save_dir
                    )
                else:
                    # Use Dask for all other levels
                    task_index.append(
                        delayed(save_tile_img)(
                            i, j, stride=patch_size, store=img_zarr, save_dir=save_dir
                        )
                    )

        patch_size *= 2

    print("Total Tasks ", len(task_index))

    if task_index:
        # from dask.distributed import Client
        # client = Client()
        futures = client.compute(task_index)
        client.gather(futures)

    return save_dir


# run()
