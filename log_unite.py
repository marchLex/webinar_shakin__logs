import os
import gzip
import shutil


class LogExtract:
    def __init__(self, log_file, log_directory):
        """
        :param log_file: file name
        :param log_directory: directory name
        """
        self.log_file = log_file
        self.log_directory = log_directory

    def get_from_gz(self, archive):
        log_file_path = f'{self.log_directory}/{self.log_file}'
        with gzip.open(archive, 'rb') as f_in:
            with open(log_file_path, 'ab') as f_out:
                shutil.copyfileobj(f_in, f_out)

    def worker(self):
        path = os.getcwd() + '\\' + self.log_directory
        files = os.listdir(path)

        for file in files:
            if file.endswith('.gz'):
                self.get_from_gz(path + '/' + file)
        print('Access files union complete!')


if __name__ == '__main__':
    extract = LogExtract('log_filename', 'folder_name')
    extract.worker()
