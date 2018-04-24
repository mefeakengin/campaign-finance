import os
import requests
import StringIO
import urllib2
import zipfile
import gzip


#Download and write the file
def download_file(url, path):

    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')] # Workaround for AWS
    urllib2.install_opener(opener)

    connection = urllib2.urlopen(url)
    fh = open(path, "w")
    fh.write(connection.read())
    if connection: connection.close()
    fh.close()

    return

# extract the file and remove the zip
def extract_file(source_path, extract_path):

    zfile = zipfile.ZipFile(source_path)
    print "zipfile namelist, ", zfile.namelist()
    for f in zfile.namelist():
        zfile.extract(f, extract_path)
	# with zipfile.ZipFile(source_path, 'r') as zf:
	# 	with open(extract_path, "w") as f:  # open the output path for writing
	# 		f.write(zf.read(zf.namelist()[0]))
	return


# extract multiple data files of same type and merge them
def extract_merge_files(source_path, extract_path, merge_path):
    zfile = zipfile.ZipFile(source_path)

    os.system("touch " + merge_path)
    print "extract_path, ", extract_path
    print "source_path, ", source_path

    with zipfile.ZipFile(source_path, "r") as z:
        z.extractall(extract_path)

    for filename in zfile.namelist():
        # in_size = (os.stat(fname).st_size / 1000000)
        # try:
        #     out_size = (os.stat(file).st_size / 1000000)
        # except OSError:
        #     out_size = 0
        # if (in_size + out_size) > 75:
        #     file = re.split('\.', outputfile)[0] + '_' + str(counter) + '.xml'
        #     counter = counter + 1

        with open(merge_path, 'a') as outfile:
            with open(extract_path + '/' + filename) as infile:
                print "extract_path, ", extract_path
                print "file_name, ", filename
                for line in infile:
                    outfile.write(line)
    os.system("rm -r " + extract_path)
    return


# fix the character issues of the files, particularly issues with UTF8 characters.
def clean_file(path):
	temp_path = path + ".temp"
	os.system("iconv -f iso-8859-1 -t utf-8 < " + path + " > " + temp_path)
	# os.system("mv " + temp_path + " " + path)

def prepare_fec_files(data_dir, filenames, urls):
    # Download and extract all files
    for i in range(0, len(urls)):
        url = urls[i]
        filename = filenames[i]
        print url
        print filename
        final_path = data_dir + '/' + filename
        download_path = final_path + '.zip'
        download_file(url, download_path)
        extract_file(download_path, final_path)
        clean_file(final_path)
        os.remove(download_path)
        print "File " + filename + " is ready"


def download_fec(data_dir, download_source):
    # populate data links of format ftp://ftp.fec.gov/FEC/2018/cm18.zip
    years = [str(i) for i in range(1980, 2020, 2)]
    data_prefixes = ['cm', 'cn', 'ccl', 'oth', 'pas2', 'indiv'] #operating expenditures are ignored
    
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    
    urls = []
    filenames = []
    for year in years:
        for prefix in data_prefixes:
            if prefix != 'ccl' or int(year) >= 2000: #CCL only exists after 2000
                urls.append(download_source + '/' + year + '/' + prefix + year[2:] + '.zip')
                filenames.append(prefix + year[2:] + '.txt')
    print filenames

    prepare_fec_files(data_dir, filenames, urls)


def prepare_senate_files(data_dir, filenames, urls):
    # Download and extract all files
    for i in range(0, len(urls)):
        url = urls[i]
        filename = filenames[i]
        final_path = data_dir + '/' + filename + '.xml'
        extract_path = data_dir + '/' + filename + '_extracted'
        download_path = data_dir + '/' + filename + '.zip'
        download_file(url, download_path)
        extract_merge_files(download_path, extract_path, final_path)

        # clean_file(extract_path)
        os.remove(download_path)
        print "File " + filename + " is ready"


def download_senate(data_dir, download_source):
    # populate data links of format http://soprweb.senate.gov/downloads/2018_1_LD203.zip
    first_year = 2008 # There is an edge case for first year
    last_year = 2018 # There is an edge case for last year
    years = [i for i in range(first_year, last_year + 2, 2)]
    quarters = ['1', '2', '3', '4']  # operating expenditures are ignored
    quarters_first_year = ['2', '3', '4'] # Only available quarters for 2008
    quarters_last_year = ['1', '2'] # Currently 2018 only has the 1st, 2nd, 3rd quarters
    data_descriptor = 'LD203'

    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    urls = []
    foldernames = []
    for year in years:
        if year not in [first_year, last_year]:
            for quarter in quarters:
                urls.append(download_source + '/' + str(year) + '_' + quarter + '_' + data_descriptor + '.zip')
                foldernames.append(data_descriptor + '_' + str(year)[2:] + '_' + quarter)

                # filenames.append(data_descriptor + '_' + str(year)[2:] + '_' + quarter + '.xml')
        elif year == first_year:
            for quarter in quarters_first_year:
                urls.append(download_source + '/' + str(year) + '_' + quarter + '_' + data_descriptor + '.zip')
                foldernames.append(data_descriptor + '_' + str(year)[2:] + '_' + quarter)
        elif year == last_year:
            for quarter in quarters_last_year:
                urls.append(download_source + '/' + str(year) + '_' + quarter + '_' + data_descriptor + '.zip')
                foldernames.append(data_descriptor + '_' + str(year)[2:] + '_' + quarter)


        prepare_senate_files(data_dir, foldernames, urls)

main_dir = os.getcwd()

# main_dir = os.getcwd()
# fec_download_source = 'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads'
# fec_data_dir = main_dir + '/data/fec'
# os.system('mkdir ' + fec_data_dir)
# download_fec(fec_data_dir, fec_download_source)

ld_download_source = 'http://soprweb.senate.gov/downloads'
ld_data_dir = main_dir + '/data/senate'
os.system('mkdir ' + ld_data_dir)
download_senate(ld_data_dir, ld_download_source)

