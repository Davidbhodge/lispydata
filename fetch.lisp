(defpackage :lispydata/fetch
  (:use #:cl
	#:org.mapcar.ftp.client
	#:data-format-validation)
  (:export #:fetch
	   #:generate-csv-spec
	   #:*lispydata-home*))

(in-package :lispydata/fetch)
;;
;; The summary: . The type of the download is governened by the URL scheme (HTTP(S), FTP, FILE) and decompression (where necessary) is inferred from the file suffix. Currently gzip, bzip, .Z  .tar and .tar.gz  are supported




(eval-when (:execute :load-toplevel :compile-toplevel)
  (defparameter *lispydata-home* "/tmp/lispydata/"
    "The default destination for transferring files")
  (uiop:ensure-all-directories-exist (list *lispydata-home*)))

;; a remote file has a URI. From the URI we derive the file name,
;;the host where the file lives (for FTP), the transfer protocol
;;(FTP or HTTP(s)) , the archive type (or nil for a plain file) and a specific destination path for the file. Note that compressed files when output will have the siffix removed
;; Usually this will default to  *lispydata-home*.

(defclass remote-file ()
  ((URI                   :accessor get-URI                  :initarg :URI)
   (name                  :accessor get-name                 :initarg :name)
   (host                  :accessor get-host                 :initarg :host)
   (archive-type          :accessor get-archive-type         :initarg :archive-type)
   (file-type             :accessor get-file-type            :initarg :file-type)
   (protocol              :accessor get-protocol             :initarg :protocol)
   (destination-directory :accessor get-destination          :initarg :destination-directory)
   (destination-pathname  :accessor get-destination-pathname :initarg :destination-pathname)))

(defun make-remote-file (file)
  (let* ((uri (quri:uri file))
	 (protocol (quri:uri-scheme uri))
	 (name (%uri-file uri))
	 (archive-type (%archive-protocol name))
	 (hostname (quri:uri-host uri))
	 (destination-pathname (trim-archive-suffix
				(concatenate 'string
					     *lispydata-home*
					     hostname
					     (quri:uri-path uri))
				archive-type)))
    
    (uiop:ensure-all-directories-exist (list destination-pathname))
    
    (make-instance 'remote-file
		   :URI uri
		   :name name
		   :protocol (%protocol protocol)
		   :host hostname
		   :archive-type archive-type
		   :file-type (%infer-file-type name)
		   :destination-directory  *lispydata-home*
		   :destination-pathname destination-pathname)))

(defun trim-archive-suffix (name archive-type)
  (let ((suffix (cdr (assoc archive-type '( (:gzip . ".gz")
					   (:z . ".Z")
					   (:tar . ".tar")
					   (:tarball . ".tar.gz")
					   (:zip . ".zip")
					   (:bzip . ".bz2"))))))
    (if suffix
	(subseq name 0 (- (length name) (length suffix)))
	name)))


;;  
(defgeneric %fetch (file protocol)
  (:documentation "fetch the file from a remote host using the appropriate protocol")
  (:method ((file remote-file)   (protocol (eql :FTP)))
	  (%ftp-retrieve file))
  (:method ((file remote-file) (protocol ( eql :HTTP)))
	  (let* ((filename (quri:render-uri (get-uri file))))
	    (trivial-download:download filename (get-destination-pathname file))))
  (:method ((file remote-file) (protocol (eql :file)))
	  (uiop:copy-file (get-name file) (get-destination-pathname file))))



(defun %ftp-retrieve (file &key (port 21) (username "anonymous") (password "lispydata"))
  "This taken from the example cl-ftp client."
  
  (ftp.client:with-ftp-connection (conn :hostname (get-host file) 
			     :port port
			     :username username
			     :password password
			     :passive-ftp-p t)
    (ftp.client:send-cwd-command conn (%uri-path-only (get-uri file)))
    (ftp.client:retrieve-file conn (get-name file)
		   (get-destination-pathname file)
		   :type :binary :if-exists :supersede)))

(defun %infer-file-type (name-string)
  "return the file type(s) of the file designated by name-string. The order of this is important in the case of things like tar.gz."
  (loop  
     for (name  type) = (multiple-value-list (uiop:split-name-type name-string))  
     until  (equal  type :unspecific)
     collect  (alexandria:make-keyword (string-upcase type))
     do (setf name-string name)) )

(defun %type-contains (file-type type)
  "helper for determining archive types"
  (member type file-type :test #'equal))

(defun %archive-protocol (filename)
  "determine what archiver (if any) we should use"
  (let ((file-type (%infer-file-type filename )))
    (cond
      ((or (%type-contains file-type :gz) (%type-contains file-type :z))
       (if (%type-contains file-type :tar)
	   :tarball
	   :gzip) )
      ((%type-contains file-type :zip) :zip)
      ((%type-contains file-type :bz2 ):bzip)
      (t :ignore))))

(defun %uri-file (uri)
  "return the filename part of the uri"
  (let* ((path (quri:uri-path uri))
	 (slash (position #\/ path :from-end t)))
    (if slash
	(subseq path (1+  slash))
	path)))

(defun %uri-path-only (uri)
  "return the path of the uri (without the filename)"
  (let* ((path (quri:uri-path uri))
	 (slash (position #\/ path :from-end t)))
    (if slash
	(subseq path 0 slash)
	path)))

(defun %protocol (str)
  "what is the protocol being used for file transfer. Note that HTTP and HTTPS are collapsed together. This does not effect the actual transfer, just means I can use a single method for implementation"
  (cond
    ((string= str "http") :http)
    ((string= str "https") :http)
    ((string= str "ftp") :ftp)
    ((string= str "file") :file)
    (t nil)))

;;

(defun  %extract-tarball (file &optional (tar-only nil))
  "Extract a tarball (.tar.gz) file to a directory (*lispydata-home*). Member files are placed in that directory"
  (with-open-file (tarball-stream (get-name file)
				  :direction :input
				  :element-type '(unsigned-byte 8))
    ;; the only way to tell archive where to put files is by binding default-pathname-defaults
    (let ((*default-pathname-defaults* (get-destination-pathname file) ))
      (archive::extract-files-from-archive
       (archive:open-archive 'archive:tar-archive
			     (if tar-only
				 tarball-stream
				 (chipz:make-decompressing-stream 'chipz:gzip tarball-stream))

			     :direction :input))))
  (uiop:delete-file-if-exists (get-name file)))


(defparameter *archive-methods-alist* '(( :gzip . chipz:gzip)
					( :zip . chipz:zlib)
					( :bzip . chipz:bzip2)
					( :plain . :ignore)))
(defun %archive-method-decode (amethod)
  (cdr (assoc amethod *archive-methods-alist*)))


(defun %extract (file)
  (case (get-archive-type file)
    (:ignore t)
    (:tarball (%extract-tarball file) )
    (:tar (%extract-tarball file t))
    (otherwise
     (with-open-file  (archive-stream (get-name file)
				      :direction :input
				      :element-type '(unsigned-byte 8))
       (with-open-file (stream (get-destination-pathname file)
			       :direction :output
			       :element-type '(unsigned-byte 8)
			       :if-exists :supersede)
	 (chipz:decompress stream
			   (%archive-method-decode
			    (get-archive-type file) )
			   archive-stream)))
    (uiop:delete-file-if-exists (get-name file)) )))



(defparameter *csv-type-alist* '((number . dfv::number)
				 (date    . dfv:date)
				 (category . dfv::string)
				 (string . dfv::string))
  "an alist to map df column types to the parsing library")

(defun %process-type (type field)
  "try to guess the type of a field. note the special case of category fields"
  (let ((dfv-type (cdr (assoc type *csv-type-alist*))))
    
    (handler-case (dfv:parse-input dfv-type field)
      (dfv:invalid-format(condition) (declare (ignore condition)) nil)
      (dfv::invalid-number () nil)
      (:no-error (x) (declare (ignore x))
		 type))))

(defun %infer-column-type (field)
  "for the string in field see if its one of
     - a number
     - a date
     - a string representing a categorical variable"


  (loop
     for spec in (mapcar #'car (remove 'category *csv-type-alist* :key #'car))
     for type = (%process-type spec field)
     when type do (return type))
  ) ; if we have not found anything, it must be a string. we should never get there though

(defun %blank-csv-line (line)
  (every (lambda (field) (equal field "")) line ))

(defun %collect-column-names-as-keywords (csv-file)
  "skip any leading blank lines and read the column headers. the requirement is the first non blank line are the column headers."
  (loop
     for line = (fare-csv:read-csv-line csv-file)
     unless (%blank-csv-line line)
     do (return (mapcar (lambda (field) (alexandria:make-keyword field)) line))))

(defun %infer-column-types (csv-file)
  "at the moment just use the first data row as the prototypes for guessing the column types. we might suck in a few more lines if this is not too reliable."
  (let ((original-csv-position (file-position csv-file))
	(line (loop for csv-line = (fare-csv:read-csv-line csv-file)
		 unless (%blank-csv-line csv-line) do (return csv-line))))
    (file-position  csv-file original-csv-position) ; rewind the file to the first data line
    (mapcar #'%infer-column-type line)))

(defun %make-column-vector (type)
  (ecase type
    (number (make-array 1000
			 :fill-pointer 0
			 :adjustable t
			 :element-type t))
    (date (make-array 1000
		       :fill-pointer 0
		       :adjustable t
		       :element-type 'fixnum))
    (string (make-array 1000
			 :fill-pointer 0
			 :adjustable t))
    (category (make-array 1000
			 :fill-pointer 0
			 :adjustable t
			 :element-type 'keyword))))


(defun %parse-input-field (type field)
  (if (equal type 'category)
      (alexandria:make-keyword field)
      (dfv:parse-input  (cdr (assoc type *csv-type-alist*)) field)))

(defun %process-csv-lines (csv-file column-types columns)
  " read through the lines in the csv-file and process them"
  (loop for line = (fare-csv:read-csv-line csv-file)
	 until (null line)
	 do (loop for field in line and
	       type in column-types and
	       column in columns do
		 (vector-push-extend (%parse-input-field type field) column))))

(defun %read-csv-guess-spec (file )
  " When its a CSV with column headers use the column headers and then attempt to infer the types of each column."
  (with-open-file ( csv-file (get-destination-pathname file)   :direction :input )
    (let* ((column-names (%collect-column-names-as-keywords csv-file))
	   (column-types (%infer-column-types csv-file))
	   (columns (loop for type in column-types  collect (%make-column-vector type))))
      (%process-csv-lines csv-file column-types columns)

      (dframe:make-df column-names columns ))))

(defun %read-csv-with-spec (file  spec)
  " "
  (with-open-file ( csv-file (get-destination-pathname file)  :direction :input )
    (let* ((skip (if (member 'skip spec) (setf spec (remove 'skip spec)) nil))
	   (column-names (mapcar #'car spec))
	   (column-types (mapcar #'cdr spec))
	   (columns (loop for type in column-types  collect (%make-column-vector type))))
      ;; skip blank lines and the header line.
      (when skip
	(loop while (%blank-csv-line (fare-csv:read-csv-line csv-file))))
      (%process-csv-lines csv-file column-types columns)
      
      (dframe:make-df column-names columns ))))
;;
;; a CSV spec is currently an plist of the form '( (:skip | :noheading)  (colname1 . :type) (Colname2 . :type) ....)
;; where skip means skip existing column headings
;;       noheading means don't look for column headings
;;       type is one of
;;       number  - put into a type t vector, this may be coerced into double-floats
;;       date    - a fixnum
;;;      category - symbol
;;;      string  - string.
;;; if you don;t use a spec for the csv it does its best to guess types. Categorical values are hard to distinguish from strings - so a simple heuristic is applied. If the CSV field is < 20 long, we guess is a category otherwise its a string.
;; Note: If your file does not have column names, you must use a spec
;; Note: we can't guess categorical variables very well. So we don't try . If a column is categorical you have to use a spec to denote it

(defun %csv-to-df (file &optional (spec nil))
  
  (if spec
      (%read-csv-with-spec file spec)
      (%read-csv-guess-spec file)))

(defun %flat-file-to-df (file spec)
  "apply a spec to parse out the file and turn it into a df"
  (error "calling flat-file-to-df with ~a~%" file))

(defun %file-to-df (remote-file &optional (spec nil ))
  "convert a file into a dataframe. "
  (case (first (%infer-file-type (get-name remote-file)))
    (:csv (%csv-to-df remote-file spec ))
    (otherwise (error "flat files not implemented"))))

(defun fetch (file &key (spec nil) (cache nil))
  "fetch a file, unpack it if needed and then if its a csv turn it into a dataframe. If cache non nil, don't do the download"
  (let ((file-to-fetch (make-remote-file file)))
    (unless cache
      (%fetch file-to-fetch (get-protocol file-to-fetch))
      (%extract file-to-fetch))
    (%file-to-df file-to-fetch spec)))


(defun generate-csv-spec (file )
  "Conveninece function to generate a valid spec to read a CSV, makes it easier to create a spec when we can't guess the contents"
  (with-open-file ( csv-file  file   :direction :input )
    (let* ((column-names (%collect-column-names-as-keywords csv-file))
	   (column-types (%infer-column-types csv-file)))
      
      (mapcar #'cons column-names column-types))))
