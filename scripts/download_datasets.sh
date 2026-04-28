# MOST OF THE CODE HERE COMES FROM SOSD -> https://github.com/learnedsystems/SOSD

# Calculate md5 checksum of FILE and stores it in MD5_RESULT
function get_checksum() {
   FILE=$1
   if [ -x "$(command -v md5sum)" ]; then
      # Linux
      MD5_RESULT=`md5sum ${FILE} | awk '{ print $1 }'`
   else
      # OS X
      MD5_RESULT=`md5 -q ${FILE}`
   fi
}

function download_file_zst() {
   FILE=$1;
   CHECKSUM=$2;
   URL=$3;

   # Check if file already exists
   if [ -f ${FILE} ]; then
      # Exists -> check the checksum
      get_checksum ${FILE}
      if [ "${MD5_RESULT}" != "${CHECKSUM}" ]; then
         wget -O - ${URL} | zstd -d > ${FILE}
      fi
   else
      # Does not exists -> download
      wget -O - ${URL} | zstd -d > ${FILE}
   fi
   # Validate (at this point the file should really exist)
   get_checksum ${FILE}
   if [ "${MD5_RESULT}" != "${CHECKSUM}" ]; then
      echo "error checksum does not match: run download again"
      exit -1
   else
      echo ${FILE} "checksum ok"
   fi
}

function download_coordinate_file() {
   FILE_OUTPUT=$1
   FILE_COMPRESSED=$2
   URL=$3

   # Download if not already present
   if [ ! -f "$FILE_COMPRESSED" ]; then
      echo "Downloading file..."
      wget -O "$FILE_COMPRESSED" "$URL"
   else
      echo "File already exists, skipping download."
   fi

   gunzip -c "$FILE_COMPRESSED" > extracted_file
   
   echo "Processing file..."
   tail -n +8 extracted_file | sort -k3,3n | awk '{print $4}' > "$FILE_OUTPUT"
   rm extracted_file "$FILE_COMPRESSED"

   # Remove duplicates without sorting
   awk '!seen[$0]++' "$FILE_OUTPUT" > "${FILE_OUTPUT}.tmp" && mv "${FILE_OUTPUT}.tmp" "$FILE_OUTPUT"
}

function main() {
   echo "downloading data ..."
   mkdir -p data
   cd data

   # Format: download_file <file_name> <md5_checksum> <url>
   if [ ! -f wiki_ts_200M_uint64 ]; then
      download_file_zst wiki_ts_200M_uint64 4f1402b1c476d67f77d2da4955432f7d https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/SVN8PI
   fi
   
   if [ ! -f books_800M_uint64 ]; then
      download_file_zst books_800M_uint64 8708eb3e1757640ba18dcd3a0dbb53bc https://www.dropbox.com/s/y2u3nbanbnbmg7n/books_800M_uint64.zst?dl=1
   fi

   # if [ ! -f osm_cellids_800M_uint64 ]; then
   #    download_file_zst osm_cellids_800M_uint64 70670bf41196b9591e07d0128a281b9a https://www.dropbox.com/s/j1d4ufn4fyb4po2/osm_cellids_800M_uint64.zst?dl=1
   # fi

   # if [ ! -f fb_200M_uint64 ]; then
   #    download_file_zst fb_200M_uint64 3b0f820caa0d62150e87ce94ec989978 https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7 
   # fi

   # download coordinate files
   if [ ! -f USA-road-d.USA.co.txt ]; then
      download_coordinate_file "USA-road-d.USA.co.txt" "USA-road-d.USA.co.gz" "https://www.diag.uniroma1.it/~challenge9/data/USA-road-d/USA-road-d.USA.co.gz"
   fi

   echo "Download done"
   cd ..
}

function shuffle_datasets() {
   echo "shuffling datasets ..."
   cd data
   for f in \
      wiki_ts_200M_uint64 \
      books_800M_uint64   \
   do
      python3 ../scripts/shuffle_dataset.py "$f" "shuffled_${f}"
   done
   echo "Shuffling done"
   cd ..
}

# Run
main
shuffle_datasets