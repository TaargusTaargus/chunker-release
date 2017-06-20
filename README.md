#chunker-release

Release version of chunker.



               COMMANDS: 
               chunker upload [OPTIONS...] DIR

               chunker download [OPTIONS...] DIR

               chunker list DB-NAME

               chunker help COMMAND


              GENERAL OPTIONS:

                -s mode
                  determines storage location: box, drive (default)
                -m db
                  specify the database file to write to/read from
                -v, --verbose
                  more verbose output from chunk
                -t # of threads
                  request threading with specified number of threads


              UPLOAD ONLY OPTIONS:
                -cf, --collapse
                  collapse the filesystem on upload
                -e, --encrypt
                  enable encryption
                -c size
                  chunk all files within [size] byte chunks
                -f, --force
                  forces  reupload (might cause duplication of file contents)
                -a, --fast
                  fastest mode for upload (-cf -c 10000000)
								
     
