import os
import glob
import optparse
import configparser
import uuid

import logging.config
from datetime import datetime, timedelta

import pandas as pd
from dateutil.parser import parse as du_parse
from pytz import timezone
from shapely.geometry import shape
from uuid import uuid4
from pathlib import Path
from xmrgprocessing.xmrg_process import xmrg_process
from xmrgprocessing.boundary import boundariesparse
from xmrgprocessing.xmrgfileiterator.xmrg_file_iterator import xmrg_file_iterator
from xmrgprocessing.xmrg_utilities import build_filename

from CSVDataSaver import nexrad_csv_saver


def load_boundaries_from_geojson(geojson_directory):
    logger = logging.getLogger()
    watershed_list = glob.glob(os.path.join(geojson_directory, "*.json"))
    boundaries = boundariesparse.Boundary(unique_id=uuid.uuid4())
    boundaries.parse_boundaries_file(geojson_directory)
    return boundaries
    """
    boundaries_tuples = []
    for watershed in watershed_list:
        try:
            json_dataframe = gpd.read_file(watershed)

            for ndx, row in json_dataframe.iterrows():
                bnd_json = geojson.loads(to_geojson(row['geometry']))
                boundaries_tuples.append((row['Name'], bnd_json))

        except Exception as e:
            logger.exception(e)

        #with open(watershed, "r") as watershed_obj:
        #    geo_json = geojson.load(watershed_obj)
        #    name = geo_json['features'][0]['properties']['Name']
        #    polygon = geo_json['features'][0]['geometry']
        #    boundaries.append((name, polygon))
    return boundaries_tuples
    """


def build_missing_date_list(
    precipitation_output_directory: Path, start_date: datetime, end_date: datetime
):
    file_list = precipitation_output_directory.glob("*.csv")
    missing_data_list = []
    # Create the complete list of expected hourly datetimes
    expected_times = pd.date_range(start=start_date, end=end_date, freq="h")
    for file in file_list:
        csv_df = pd.read_csv(file, parse_dates=["Start Time", "End Time"])
        # Find missing hourly datetimes
        missing_times = expected_times.difference(csv_df["Start Time"])
        # If we have existing missing date times, let's see if the latest missing_times has an entry we don't already have.
        if len(missing_data_list):
            new_missing_times = missing_times.difference(missing_data_list)
            if len(new_missing_times):
                missing_data_list.extend(
                    pd.to_datetime(new_missing_times).to_pydatetime().tolist()
                )
        missing_times_list = pd.to_datetime(missing_times).to_pydatetime().tolist()
        if len(missing_data_list) == 0:
            missing_data_list = [date_time for date_time in missing_times_list]

    missing_data_list.sort()
    return missing_data_list


def main():
    parser = optparse.OptionParser()

    parser.add_option(
        "--LastNHours",
        dest="last_n_hours",
        help="Number of hours of XMRG files to download.",
        default=None,
        type="int",
    )

    parser.add_option(
        "--StartDate",
        dest="start_date",
        help="The starting date to process XMRG files.",
        default=(datetime.now() - timedelta(hours=72)).strftime("%Y-%m-%d %H:00:00"),
    )

    parser.add_option(
        "--EndDate",
        dest="end_date",
        help="The ending date to process XMRG files.",
        default=datetime.now().strftime("%Y-%m-%d %H:00:00"),
    )

    parser.add_option(
        "--DownloadDirectory",
        dest="download_directory",
        help="Directory to download XMRG files to.",
        default=None,
    )

    parser.add_option(
        "--ConfigurationFile",
        dest="config_file",
        help="Configuration Settings.",
        default=None,
    )

    parser.add_option(
        "--FillGaps",
        dest="fill_gaps",
        action="store_true",
        default=False,
        help="Configuration Settings.",
    )

    options, args = parser.parse_args()

    now_time = datetime.now()
    config_file = configparser.ConfigParser()
    config_file.read(options.config_file)

    log_config = config_file.get("logging", "config_file")
    logging.config.fileConfig(log_config)
    logger = logging.getLogger()
    logger.info("Logging started.")

    ll, ur = config_file.get("xmrg", "bbox").split(";")
    ll = ll.split(",")
    ll[0] = float(ll[0])
    ll[1] = float(ll[1])
    ur = ur.split(",")
    ur[0] = float(ur[0])
    ur[1] = float(ur[1])

    url = config_file.get("xmrg", "url")
    save_all_precip_values = config_file.get("xmrg", "save_all_precip_values")
    delete_source_file = config_file.get("xmrg", "delete_source_file")
    delete_compressed_source_file = config_file.get(
        "xmrg", "delete_compressed_source_file"
    )
    download_directory = config_file.get("xmrg", "download_directory")
    xmrg_base_directory = config_file.get("xmrg", "base_xmrg_directory")
    kml_output_directory = config_file.get("xmrg", "kml_output_directory")
    base_log_directory = config_file.get("logging", "logging_directory")
    boundary_directory = config_file.get("watershed", "directory")
    sqlite_file = config_file.get("xmrg", "database_file")
    worker_process_count = config_file.getint("xmrg", "worker_process_count")
    precipitation_temp_output_directory = Path(
        config_file.get("xmrg", "precipitation_temp_output_directory")
    )
    precipitation_temp_output_directory.mkdir(exist_ok=True)
    precipitation_output_directory = Path(
        config_file.get("xmrg", "precipitation_output_directory")
    )
    precipitation_output_directory.mkdir(exist_ok=True)
    # data_saver = nexrad_xenia_sqlite_saver(sqlite_file)
    data_saver = nexrad_csv_saver(
        precipitation_output_directory,
        precipitation_temp_output_directory,
        "UTC",
        "US/Eastern",
    )

    boundaries = load_boundaries_from_geojson(boundary_directory)

    wkt_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "boundaries.csv"
    )
    with open(wkt_file, "w") as wkt_obj:
        for boundary in boundaries.boundaries:
            boundary_polygon = shape(boundary[1])
            wkt_obj.write(f'"{boundary[0]}","{boundary_polygon.wkt}"\n')

    # We operate in EST, nexrad files are in UTC.
    utcTZ = timezone("UTC")
    estTZ = timezone("US/Eastern")

    start_date = options.start_date
    if type(start_date) != datetime:
        start_date = du_parse(options.start_date)

    end_date = options.end_date
    if type(end_date) != datetime:
        end_date = du_parse(options.end_date)

    start_date = estTZ.localize(start_date)
    end_date = estTZ.localize(end_date)

    utc_end_date = end_date.astimezone(utcTZ)
    utc_start_date = start_date.astimezone(utcTZ)

    missing_dates = build_missing_date_list(
        precipitation_temp_output_directory, utc_start_date, utc_end_date
    )

    task_id = uuid4()
    if len(missing_dates):
        xmrg_iterator = xmrg_file_iterator(
            date_list=missing_dates, base_xmrg_path=xmrg_base_directory
        )
    else:
        xmrg_iterator = xmrg_file_iterator(
            start_date=utc_start_date,
            end_date=utc_end_date,
            base_xmrg_path=xmrg_base_directory,
        )

    xmrg_proc = xmrg_process(
        file_list_iterator=xmrg_iterator,
        data_saver=data_saver,
        boundaries=boundaries.boundaries,
        worker_process_count=worker_process_count,
        unique_id=task_id,
        source_file_working_directory=download_directory,
        output_directory=kml_output_directory,
        base_log_output_directory=base_log_directory,
        results_directory=kml_output_directory,
        kml_output_directory=kml_output_directory,
        save_all_precip_values=save_all_precip_values,
        delete_source_file=True,
        delete_compressed_source_file=True,
    )
    xmrg_proc.process(
        start_date=utc_start_date,
        end_date=utc_end_date,
        base_xmrg_directory=xmrg_base_directory,
    )

    logger.info("Logging stopped.")

    return


if __name__ == "__main__":
    main()
