import argparse
import multiprocessing
import geojson
import shapely
import datetime
import os
import rasterio
from tsd import utils, parallel
from tsd import s2_metadata_parser
from tsd import get_sentinel2, get_sentinel1, get_sentinel3

# TODO improve mirror and api selection and other pre-defined parameters
# TODO factorize the two functions
# TODO documentation
# TODO Fix everything so that it works with other satellites (S1 and S3)

spatial_resolution = {'Sentinel2': (10, 10), 'Sentinel1': (10, 3), 'Sentinel3': (500, 500)}
get_sat = {'Sentinel2': get_sentinel2, 'Sentinel1': get_sentinel1, 'Sentinel3': get_sentinel3}

def bands_files_are_valid(img, bands, d):
    """
    Check if all bands images files are valid.
    """
    return all(utils.is_valid(os.path.join(d, f"{img.filename}_band_{b}.tif")) for b in bands)


def get_series(args):
    get = get_sat[args.sat]

    with open(args.geom) as f:
        features = geojson.load(f)["features"]
        geoshape = shapely.geometry.shape(features[0]["geometry"])
        centroid = geoshape.centroid
        args.geom = utils.geojson_geometry_object(centroid.y, centroid.x,
                args.width*spatial_resolution[args.sat][0], args.height*spatial_resolution[args.sat][1])

    # list available images
    images = get.search(args.geom, args.start_date, args.end_date)

    # Removes images that are too cloudy before downloading anything
    if args.filter_cloudy is not None:
        # Retrieve SCL images that contains a cloud mask. FIXME use the GML directly since it less costly but hidden behind a request payer for now
        get.download(images, ['SCL'], args.geom, out_dir=args.outdir)
        # Check which image is cloudy
        def cloud_coverage(img):
            fname = os.path.join(args.outdir, '{}_band_SCL.tif'.format(img.filename))
            try:
                with rasterio.open(fname) as src:
                    clouds = src.read()
                    profile = src.profile
                    clouds = ((clouds == 3).sum() + (clouds == 9).sum()) / (clouds.shape[1] * clouds.shape[2])
            except rasterio.errors.RasterioIOError:
                print("WARNING: download of {} failed".format(fname))
                clouds = 1.1 # Guarantee discarding the image
            return clouds

        cloudy = [cloud_coverage(img) > args.filter_cloudy for img in images]
        images = [im for im, cloud in zip(images, cloudy) if not cloud]

    # Only keep a limited number of images because of IPOL time constraints
    if args.max > 0:
        images = images[:args.max]

    # download crops
    get.download(images, args.band, args.geom, out_dir=args.outdir)

    images = [i for i in images if bands_files_are_valid(i, args.band, args.outdir)]
    if len(images) == 0:
        # No images have been downloaded, report of a failure to the demo system
        with open("demo_failure.txt", "w") as f:
            f.write("No images found for the requested dates")
        exit(1)


def get_nearest(args):
    get = get_sat[args.sat]

    with open(args.geom) as f:
        features = geojson.load(f)["features"]
        geoshape = shapely.geometry.shape(features[0]["geometry"])
        centroid = geoshape.centroid
        args.geom = utils.geojson_geometry_object(centroid.y, centroid.x,
                args.width*spatial_resolution[args.sat][0], args.height*spatial_resolution[args.sat][1])

    # Define a search period with ± two weeks around the requested date
    args.start_date = args.date - datetime.timedelta(days=14)
    args.end_date = args.date + datetime.timedelta(days=14)

    # list available images
    images = get.search(args.geom, args.start_date, args.end_date)
    # Removes images that are too cloudy before downloading anything
    if args.filter_cloudy is not None:
        # Retrieve SCL images that contains a cloud mask. FIXME use the GML directly since it less costly but hidden behind a request payer for now
        get.download(images, ['SCL'], args.geom, out_dir=args.outdir)
        # Check which image is cloudy
        def cloud_coverage(img):
            fname = os.path.join(args.outdir, '{}_band_SCL.tif'.format(img.filename))
            try:
                with rasterio.open(fname) as src:
                    clouds = src.read()
                    profile = src.profile
                    clouds = ((clouds == 3).sum() + (clouds == 9).sum()) / (clouds.shape[1] * clouds.shape[2])
            except rasterio.errors.RasterioIOError:
                print("WARNING: download of {} failed".format(fname))
                clouds = 1.1 # Guarantee discarding the image
            return clouds

        cloudy = [cloud_coverage(img) > args.filter_cloudy for img in images]
        images = [im for im, cloud in zip(images, cloudy) if not cloud]

    # select the nearest date
    images = [min(images, key=lambda x: abs(x.date.date() - args.date.date()))]

    # download crops
    get.download(images, args.band, args.geom, out_dir=args.outdir)

    images = [i for i in images if bands_files_are_valid(i, args.band, args.outdir)]
    if len(images) == 0:
        # No images have been downloaded, report of a failure to the demo system
        with open("demo_failure.txt", "w") as f:
            f.write("No image found for the requested date (in a window of ± two weeks)")
        exit(1)


def main():
    parser = argparse.ArgumentParser(description=('TODO'))
    subparsers = parser.add_subparsers(help="subparsers")
    parser_series = subparsers.add_parser('series')
    parser_series.add_argument('--sat', choices=['Sentinel1', 'Sentinel2', 'Sentinel3', 'Landsat8'],
                               help=('satellite model'))
    parser_series.add_argument('--geom', type=str,
                               help=('path to geojson file'))
    parser_series.add_argument('-w', '--width', type=int, default=512,
                               help='width of the images produced, default 512 pixels')
    parser_series.add_argument('-l', '--height', type=int, default=512,
                               help='height of the images produced, default 512 pixels')
    parser_series.add_argument('-s', '--start-date', type=utils.valid_datetime,
                               help='start date, YYYY-MM-DD')
    parser_series.add_argument('-e', '--end-date', type=utils.valid_datetime,
                               help='end date, YYYY-MM-DD')
    parser_series.add_argument('-b', '--band', nargs='*', default=['B04'], metavar='',
                               help='space separated list of spectral bands to'
                                     ' download. No check are performed here')
    parser_series.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                                 'images'), default='')
    parser_series.add_argument('-m', '--max', type=int, help='maximum amount of images to be retrieved', default=-1)
    parser_series.add_argument('-f', '--filter-cloudy', type=float, help='Pre-filter images that are more cloudy than this fraction', default=None)
    parser_series.set_defaults(func=get_series) 


    parser_nearest = subparsers.add_parser('nearest')
    parser_nearest.add_argument('--sat', choices=['Sentinel1', 'Sentinel2', 'Sentinel3', 'Landsat8'],
                               help=('satellite model'))
    parser_nearest.add_argument('--geom', type=str,
                               help=('path to geojson file'))
    parser_nearest.add_argument('-w', '--width', type=int, default=512,
                               help='width of the images produced, default 512 pixels')
    parser_nearest.add_argument('-l', '--height', type=int, default=512,
                               help='height of the images produced, default 512 pixels')
    parser_nearest.add_argument('-d', '--date', type=utils.valid_datetime,
                               help='requested date, YYYY-MM-DD')
    parser_nearest.add_argument('-b', '--band', nargs='*', default=['B04'], metavar='',
                               help='space separated list of spectral bands to'
                                     ' download. No check are performed here')
    parser_nearest.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                                 'images'), default='')
    parser_nearest.add_argument('-f', '--filter-cloudy', type=float, help='Pre-filter images that are more cloudy than this fraction', default=None)
    parser_nearest.set_defaults(func=get_nearest) 

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
