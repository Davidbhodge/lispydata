

# LispyData #

Lispydata is an attempt to bring some R like data handling functionality to lisp.

Right now (august 2017)  its in pre-alpha and I would not recommend that you use it for the moment. Not that you can easily , as some of the dependencies are not in Quicklisp as yet. I hope to get the package out of Alpha pretty quickly.

In this release, the basic dowloading of files, handling of compression and generalinfrastructure has been implemented. 

There is minimal (ie no) error checking as yet.

LISPYDATA is organized into several packages

# FETCH: #

The Fetch package  provides file downloading capabilities.

(fetch *file* *spec* *cache*)

FIle is a fully specified *uri* that denotes the protocol and full pathname of the file to download.

Downloaded files are placed in the directory *LISPYDATA-HOME* using the directory paths found in the URI.

*cache* (default nil), when t means use the file from cache and don't try to download
*spec* (default nil) is an optional parameter which describes the format of the data in the file

### CSV Spec ###

a CSV spec is currently an plist of the form

`'( :skip  (colname1 . :type) (Colname2 . :type) ....)
`
 where skip means skip existing column headings
      
     type is one of
      * number  - put into a type t vector, this may be coerced into double-floats
      * date    - a fixnum
      * category - symbol
      * string  - string.
  
 if you don't use a spec for the csv it does its best to guess types. 
 Note: If your file does not have column names, you **must** use a spec
 Note: we can't guess categorical variables very well. So we don't try . If a column is categorical you have to use a spec to denote it.

### Flat File Spec ###
To be completed.

## Supported protocols ##

HTTP/HTTPS
FTP
FILE (local files)

## Compression Schemes Supported ##


  * .gz
  * .zip (partially supported, does not know about members yet)
  * .Z
  * .tar.gz  (partially supported, does not know about members yet)
  * .bz2

## File types ##
  Currently understands .csv files.
  Flat fixed record length files supported in the next release
  NetCDF files in the release after.
  
  


# EXPLORE: #

Descriptive stats & graphics

# MUNGE: #

 (name to be revised, I think)
Merging, slicing and dicing

