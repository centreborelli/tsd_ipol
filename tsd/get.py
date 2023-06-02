import argparse
import multiprocessing
from tsd import utils
import geojson
import shapely
import datetime
from tsd import get_sentinel2, get_sentinel1, get_sentinel3

spatial_resolution = {'Sentinel2': (10, 10), 'Sentinel1': (10, 3), 'Sentinel3': (500, 500)}
get_sat = {'Sentinel2': get_sentinel2, 'Sentinel1': get_sentinel1, 'Sentinel3': get_sentinel3}

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

    # Only keep a limited number of images because of IPOL time constraints
    if args.max > 0:
        images = images[:args.max]

    # download crops
    get.download(images, bands, aoi, mirror, out_dir, parallel_downloads, no_crop, timeout)

    images = [i for i in images if bands_files_are_valid(i, bands, out_dir)]
    if len(images) == 0:
        # No images have been downloaded, report of a failure to the demo system
        with open("demo_failure.txt", "w") as f:
            f.write("No images found for the requested dates")
        exit(1)


def get_nearest(args):
    get = get_sat[args.sat]

    # Define a search period with ± two weeks around the requested date
    args.start_date = args.date - datetime.timedelta(days=14)
    args.end_date = args.date + datetime.timedelta(days=14)

    # list available images
    images = get.search(args.geom, args.start_date, args.end_date)
    # select the nearest date
    images = [min(images, key=lambda x: abs(k.date.date() - args.date))]

    # download crops
    get.download(images, bands, aoi, mirror, out_dir, parallel_downloads, no_crop, timeout)

    images = [i for i in images if bands_files_are_valid(i, bands, out_dir)]
    if len(images) == 0:
        # No images have been downloaded, report of a failure to the demo system
        with open("demo_failure.txt", "w") as f:
            f.write("No image found for the requested date (in a window of ± two weeks)")
        exit(1)


def main():
    parser = argparse.ArgumentParser(description=('TODO'))
    subparsers = parser.add_subparsers(help="subparsers")
    parser_series = subparsers.add_parser('series')
    parser_series.set_defaults(func=get_series) 

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
    parser_series.add_argument('-m', '--max', type=int, help=('maximum amount of images ',
                                                          'to be retrieved'), default=-1)


    parser_nearest = subparsers.add_parser('nearest')
    parser_nearest.set_defaults(func=get_nearest) 
    parser_series.add_argument('--sat', choices=['Sentinel1', 'Sentinel2', 'Sentinel3', 'Landsat8'],
                               help=('satellite model'))
    parser_series.add_argument('--geom', type=str,
                               help=('path to geojson file'))
    parser_series.add_argument('-w', '--width', type=int, default=512,
                               help='width of the images produced, default 512 pixels')
    parser_series.add_argument('-l', '--height', type=int, default=512,
                               help='height of the images produced, default 512 pixels')
    parser_series.add_argument('-d', '--date', type=utils.valid_datetime,
                               help='requested date, YYYY-MM-DD')
    parser_series.add_argument('-b', '--band', nargs='*', default=['B04'], metavar='',
                               help='space separated list of spectral bands to'
                                     ' download. No check are performed here')
    parser_series.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                                 'images'), default='')

    args = parser.parse_args()

if __name__ == '__main__':
    main()
