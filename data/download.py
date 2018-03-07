import os
import urllib2
import zipfile
import gzip

#Download and write the file
def download_file(url, path):
	connection = urllib2.urlopen(url)
	fh = open(download_path, "w")
	fh.write(connection.read())
	if connection: connection.close()
	fh.close()
	return 

# extract the file and remove the zip
def extract_file(source_path, extract_path):
	with zipfile.ZipFile(source_path, 'r') as zf:
		with open(extract_path, "w") as f:  # open the output path for writing
			f.write(zf.read(zf.namelist()[0]))
	return

# fix the character issues of the files, particularly issues with UTF8 characters.
def clean_file(path):
	temp_path = path[:-4] + ".temp"
	os.system("iconv -f iso-8859-1 -t utf-8 < " + path + " > " + temp_path)
	os.system("mv " + temp_path + " " + temp_path[:-5] + ".txt")

# files_to_clean = filenames
# for filename in files_to_clean:
# 	final_path = data_dir + '/' + filename 
# 	clean_file(final_path)
# 	print "File " + filename + " is cleaned"

main_dir = os.getcwd()

# populate data links of format ftp://ftp.fec.gov/FEC/2018/cm18.zip
years = [str(i) for i in range(1980, 2020, 2)]
data_prefixes = ['cm', 'cn', 'ccl', 'oth', 'pas2', 'indiv'] #operating expenditures are ignored
web_source = 'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads'
data_dir = main_dir + '/data/fec'

if not os.path.exists(data_dir):
	os.mkdir(data_dir)

urls = []
filenames = []
for year in years:
	for prefix in data_prefixes:
		if prefix != 'ccl' or int(year) >= 2000: #CCL only exists after 2000
			urls.append(web_source + '/' + year + '/' + prefix + year[2:] + '.zip')
			filenames.append(prefix + year[2:])
print filenames

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
	