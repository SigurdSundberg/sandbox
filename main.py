from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import os
import shutil
import time
import datetime


def main():
    print("Hello, welcome to my BSc project.")
    path: str = "./data/discharge_data_100/2.11.q"
    data_df: pd.Dataframe = pd.read_csv(path, delim_whitespace=True, index_col=0, parse_dates=[0], names=["date", "discharge"])
    print(data_df.index)


class bfi_analysis():
    def __init__(self, folder_path, f, data_folder="./data/", elements=0, filetype=".q", parse1=False, parse2=False, nan_ident=-9999) -> None:
        self.df_output: pd.DataFrame                                            # Output dataframe initialized in memory
        self.folder_path: str = folder_path                                     # Relative path or absolute path to folder with data
        self.folder_list_data: List[str]                                        # List of files in folder containing the data, based on file_type
        self.folder_list_supplementary: List[str]                               # All other files, used for meta data
        self.folder_path_mgn_unparsed: str = folder_path[:-1] + "_unparsed/"    # Path used for newly unparsed files
        self.folder_path_mgn_parsed: str = folder_path[:-1] + "_parsed/"        # Path used for newly unparsed files
        self.file_type: str = filetype                                          # File ending, denoting filetype
        self.elements: int = elements                                           # Number of items in folder to be read, chooses the first n elements, defaults to entire list
        self.parse1: bool = parse1                                              # Whether we want to slice the raw data creating unparsed data
        self.parse2: bool = parse2                                              # Whether we want to interpolate the unparsed data
        self.f = f
        self.nan_ident = nan_ident                                              # NaN identifier for the dataset
        self.df_metadata: pd.DataFrame                                          # Metadata dataframe

    def _folder_management(self, folder_path) -> None:
        if os.path.exists(folder_path):  # Does the folder exist
            shutil.rmtree(folder_path)  # If yes, delete
            os.mkdir(folder_path)       # And create new
        else:
            os.mkdir(folder_path)       # Else, create new

    def _get_list_of_files(self, folder_path) -> None:
        self.folder_list_data: List[str] = []
        self.folder_list_supplementary: List[str] = []

        folder: os.DirEntry = os.scandir(folder_path)               # Reads the directory
        for file in folder:                                         # Loop over files
            if file.name[-len(self.file_type):] == self.file_type:  # Checks for files of filetype
                self.folder_list_data.append(file.name)             # Adds filenames with correct filetype to list
            else:
                self.folder_list_supplementary.append(file.name)    # Add all other files to a supplementary list
        folder.close()                                              # Close the folder to prevent mishaps

    def _create_metadata_df(self):
        self.df_metadata = pd.read_csv(self.folder_path + self.folder_list_supplementary[0], delim_whitespace=True, header=0, index_col=[0, 1])

    def _read_file_raw(self, path_to_file) -> None:
        self.df_input = pd.read_csv(path_to_file, delim_whitespace=True, parse_dates=[0], names=["date", "discharge"])  # Creates a dataframe with space as delim

    def _read_file_unparsed(self, file) -> None:
        # Read file, including meta data and hydrological year
        self.df_input = pd.read_csv(file, delim_whitespace=True, index_col=[0, 1, 2], header=0)

    def _read_file_parsed(self) -> None:
        # This may not be needed
        pass

    def _parse_unparsed_data(self) -> None:
        # Interpolate short breaks in data
        pass

    def _slice_dataframe(self, file) -> None:
        self.df_input["day"] = self.df_input["date"].dt.day
        self.df_input["month"] = self.df_input["date"].dt.month
        self.df_input["year"] = self.df_input["date"].dt.year
        indexes = list(zip(self.df_input["year"], self.df_input["month"], self.df_input["day"]))
        index = pd.MultiIndex.from_tuples(indexes, names=["year", "month", "day"])
        self.df_output: pd.DataFrame = pd.DataFrame(self.df_input["discharge"], copy=True)
        self.df_output.set_index(index, inplace=True)

        hyearstart = (self.df_output.iloc[0].name[0], 9, 1)                     # Start of the hydrological year in Norway
        hyearend = (self.df_output.iloc[-1].name[0], 8, 31)                     # End of the hydrological year in Norway
        self.df_output = self.df_output.loc[hyearstart:hyearend]                # Slice the df to fit within the hydrological years

        while True:                                                             # This will from testing always break at one point
            start_year = self.df_output.iloc[0].name[0]                         # Find the start year of the new data
            end_year = self.df_output.iloc[-1].name[0]                          # End year
            slice_head = self.df_output.loc[(start_year, 9, 1)].values == -9999  # Check if it needs to be cut
            slice_tail = self.df_output.loc[(end_year, 8, 31)].values == -9999  # Check if it needs to be cut

            start_changed = True
            end_changed = True
            if slice_head:
                hyearstart = (start_year + 1, 9, 1)                             # If head is wrong, try next year
                start_changed = False
            if slice_tail:
                hyearend = (end_year - 1, 8, 31)                                # If tail is wrong try previous year
                end_changed = False
            self.df_output = self.df_output.loc[hyearstart:hyearend]            # Slice
            end_loop = start_changed and end_changed
            if end_loop:
                break                                                           # End the While loop if valid

    def _check_nan(self) -> np.ndarray:
        # If it finds nan values in the data set, it returns the location as a tuple of the nan values
        # Potential issues is long periods of NaN values
        loc_nan: np.ndarray                                                         # This seems not needed again
        nan_values = self.df_output["discharge"] == self.nan_ident                  # Creates a series
        nr_nan_values = nan_values.value_counts()                                   # Counts values in a truth array
        if True in nr_nan_values.index:                                             # Check if any value is true
            loc_nan = nan_values[nan_values == True].index.values                   # Creates a list of tuples of NaN locations
            return loc_nan                                                          # Returns above list
        return 0                                                                    # Returns zero if no nan values

    def _create_string_metadata(self, file, supp_data) -> str:
        reg, main = file[:-2].split(".")  # ["1", "2"]              # Get reg and main numbers from the file name
        meta_data = self.df_metadata.loc[(int(reg), int(main))]     # Grab the relevant data
        meta_data = meta_data.apply(str)                            # Convert it to string for later use
        index = meta_data.index                                     # Grab the index's
        values = meta_data.values                                   # Grab the values
        temp_list = ["regno " + reg, "mainno " + main]              # Create the temporary list with reg and main in them
        for x, y in zip(index, values):
            temp_list.append(" ".join([x, y]))                      # Add all the meta data

        if not isinstance(supp_data, list):                         # Check if it is a list, if not make it
            supp_data = [supp_data]

        for ele in supp_data:
            temp_list.append(str(ele))                              # Append the wanted metadata

        return " ".join(temp_list)                                  # Return as a string

    def _write_to_file_unparsed(self, file, supp_data=[-9999] * 3):
        string_final = self._create_string_metadata(file, supp_data)  # Create metadata string, based on metadata df, and is given the supp data and the file name for ref
        path_final = self.folder_path_mgn_unparsed + file             # Create the final path for the files
        # input(path_final)
        print("Final path " + path_final)
        with open(path_final, "w") as o:                              # Write metadata and seperator to the file
            o.write("# " + string_final + "\n")
            o.write("#" + "*" * 100 + "\n")
        self.df_output.to_csv(path_final, sep=" ", mode="a")          # Append the data

    def _write_to_file_parsed(self) -> None:
        # Only writes the end results to a file containing metadata from file and BFI
        pass

    def calculate(self) -> None:
        # Executes the BFI method
        pass

    def _raw_data_management(self) -> None:
        # If the files are not parsed, run this part
        self._folder_management(self.folder_path_mgn_unparsed)
        # nan_loc = []

        self.df_input: pd.DataFrame                             # Input dataframe initialized in memory
        start_time = time.time()

        self._get_list_of_files(self.folder_path)               # Get the list of files in the wanted folder

        self._create_metadata_df()                              # Create metadata df

        if self.elements != 0:
            self.folder_list_data = self.folder_list_data[:self.elements]  # Only do for x elements, used in testing
        c = 1
        print("Number of files to parse: " + str(len(self.folder_list_data)))
        for file in self.folder_list_data:
            print("Starting working on " + self.folder_path + file)
            self._read_file_raw(self.folder_path + file)        # Read the file and write it to df_input
            self._slice_dataframe(file)                         # We no longer care for the input frame, and work exclusively with the output frame
            add = self._check_nan()
            # nan_loc.append(add)
            self._write_to_file_unparsed(file, supp_data=add)
            print("Done writing " + self.folder_path + file)
            print("Current count is: " + str(c))
            print()
            c += 1

        print("Total time used: " + str(datetime.timedelta(seconds=time.time() - start_time)))

    def _data_parsing(self) -> None:
        self._folder_management(self.folder_path_mgn_parsed)
        self._get_list_of_files(self.folder_path_mgn_unparsed)
        input()
        # print(self.folder_list_data)
        # input()
        self.df_input: pd.DataFrame                             # Input dataframe initialized in memory

        for file in self.folder_list_data:
            file_object = open(self.folder_path_mgn_unparsed + file, "r")
            # Here we want to loop over the lines in the file. Extract the meta data in the first line
            # Then pass the object to the read csv file so we can get a dataframe
            self._read_file_unparsed(file_object)
            file_object.close()

        # a = open(file, "r")
        # for line in a:
        #     if line[:2] == "#*":
        #         c = pd.read_csv(a, delim_whitespace=True, names=["Date", "Discharge"], index_col=[0, 1, 2], parse_dates=True)

    def _bfi_calculation(self) -> None:
        self._read_file_parsed()
        self.calculate()
        self._write_to_file_parsed()

    def run(self) -> None:
        if self.parse1:
            self._raw_data_management()

        if self.parse2:
            self._data_parsing()

        # If files are parsed or after parsing do the calculations
        self._bfi_calculation()

    def main(self):
        nan_loc = []
        files = []
        self._get_list_of_files()

        for file in self.folder_list_data:
            self._read_file_raw(self.folder_path + file)
            self._slice_dataframe(file)
            add = self._check_nan()
            nan_loc.append(add)
            files.append(file)

        for x, y in zip(files, nan_loc):
            print(x, y)

    def main2(self):
        folder_path = "./data/"
        data_folder = "discharge_data_100/"
        filetype = ".q"
        folder_list_data = []
        folder_list_supplementary = []

        data, supp = self._get_list_of_files()
        sample_df = self.sample_data(folder_path + data_folder + data[0])  # A single dataframe used as a sample to save time

        mgn_path = folder_path + data_folder[:-1] + "_parsed/"
        self._folder_management(mgn_path)

        supp_file_path = folder_path + data_folder + supp[0]
        station_df = self._create_metadata_df(supp_file_path)  # df of the .txt file with index (reg, main)
        for file in data:
            self._write_to_file_unparsed(file, mgn_path, station_df, sample_df)


if __name__ == "__main__":
    path = "./data/discharge_data_100/"
    run = bfi_analysis(path, lambda x: x, elements=0, parse1=False, parse2=False)
    run.run()
