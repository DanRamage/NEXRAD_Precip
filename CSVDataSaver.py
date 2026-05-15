import os
import logging

import pytz
from xmrgprocessing.xmrg_results import xmrg_results
from xmrgprocessing.xmrgdatasaver.nexrad_data_saver import precipitation_saver
from datetime import datetime
from pandas import read_csv
from pathlib import Path

class nexrad_csv_saver(precipitation_saver):
    def __init__(self, output_directory: Path,
                 temp_directory: Path,
                 source_tz: str,
                 destination_tz: str):
        self._logger = logging.getLogger()

        self._new_records_added = 0
        self._boundary_output_files = {}
        self._now_date_time = datetime.now()
        self._finalized_filenames = []
        #This is where we process the files before moving them to the final destination. This can be
        #an NFS so we want to do all processing locally first to avoid any file corruption.
        self._temp_directory = temp_directory
        #The directory where the finalized file will be stored.
        self._output_directory = output_directory
        self._src_tz = pytz.timezone(source_tz)
        self._dst_tz = pytz.timezone(destination_tz)
        self._precip_files = {}
    @property
    def new_records_added(self):
        return self._new_records_added

    @property
    def csv_filenames(self):
        return self._finalized_filenames

    def save(self, xmrg_results_data: xmrg_results):
        '''
        Saves the xmrg_results_data to the CSV output file.
        :param xmrg_results_data:
        :return:
        '''
        for boundary_name, boundary_results in xmrg_results_data.get_boundary_data():
            if boundary_name not in self._precip_files:
                cleaned_boundary_name = boundary_name.replace(' ', '_')
                file_path = self._temp_directory / f"{cleaned_boundary_name}_unsorted.csv"
                write_header = False
                if not file_path.exists():
                    write_header = True
                precip_file_obj = open(file_path, "a")
                self._precip_files[boundary_name] = precip_file_obj
                if write_header:
                    precip_file_obj.write("Area,Start Time,End Time,Weighted Average\n")

            precip_file_obj = self._precip_files[boundary_name]
            try:

                avg = boundary_results['weighted_average']
                #outstring = "%s,%s,%s,%f\n" % (boundary_name,xmrg_results_data.datetime,xmrg_results_data.datetime,avg)
                #self._output_file_obj.write(outstring)
                utc_datetime = self._src_tz.localize(datetime.strptime(xmrg_results_data.datetime, "%Y-%m-%dT%H:%M:%S"))
                #local_datetime = utc_datetime.astimezone(self._dst_tz).strftime("%Y-%m-%dT%H:%M:%S")
                precip_file_obj.write(f"{boundary_name},{utc_datetime},{utc_datetime},{avg:0.6f}\n")
                #self._output_file_obj.write(f"{boundary_name},{local_datetime},{local_datetime},{avg:0.6f}\n")
            except Exception as e:
                self._logger.exception(e)

    def finalize(self):
        """
        This function is for us to clean up before the script exits.
        :return:
        """
        #Close the open files.
        for boundary_name, precip_file_obj in self._precip_files.items():
            precip_file_obj.close()
        try:
            for boundary_name, precip_file_obj in self._precip_files.items():
                unsorted_filename = precip_file_obj.name
                directory, filename = os.path.split(unsorted_filename)
                filename = filename.replace("_unsorted.csv", ".csv")
                self._logger.info(f"Sorting file file: {unsorted_filename} into file: {filename}")
                final_filename = Path(directory) / filename

                unsorted_pd_df = read_csv(unsorted_filename,
                                 dtype={
                                     "Area": str,
                                     "Start Time": str,
                                     "End Time": str,
                                     "Weighted Average": str
                                 },
                                 parse_dates=["Start Time", "End Time"],
                                 keep_default_na=False
                                 )
                if final_filename.exists():
                    pd_df = read_csv(final_filename,
                                     dtype={
                                         "Area": str,
                                         "Start Time": str,
                                         "End Time": str,
                                         "Weighted Average": str
                                     },
                                     parse_dates=["Start Time", "End Time"],
                                     keep_default_na=False
                                     )
                    result = (
                        pd_df
                        .set_index("Start Time")
                        .combine_first(unsorted_pd_df.set_index("Start Time"))
                        .reset_index()
                    )
                    sorted_df = result.sort_values(by='Start Time')
                else:
                    sorted_df = unsorted_pd_df.sort_values(by='Start Time')
                sorted_df.to_csv(final_filename, index=False)
                destination_filename = self._output_directory / filename
                self._logger.info(f"Deleting temp file: {unsorted_filename}")
                final_filename.replace(destination_filename)
        except Exception as e:
            self._logger.exception(e)

