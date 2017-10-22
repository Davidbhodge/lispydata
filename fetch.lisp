(defpackage :lispydata/fetch
  (:use #:cl
	#:org.mapcar.ftp.client
	#:data-format-validation
	#:cl-data-frame)
  (:export #:fetch
	   #:generate-csv-spec
	   #:*lispydata-home*))

(in-package :lispydata/fetch)
;;
;; The summary: The type of the download is governed by the URL scheme (HTTP(S), FTP, FILE) and decompression (where necessary) is inferred from the file suffix. Currently gzip, .Z  .tar and .tar.gz  are supported




(eval-when (:execute :load-toplevel :compile-toplevel)
  (defparameter *lispydata-home* "~/lispydata/"
    "The default destination for transferring files")
  (uiop:ensure-all-directories-exist (list *lispydata-home*)))

;; a remote file has a URI. From the URI we derive the file name,
;;the host where the file lives (for FTP), the transfer protocol
;;(FTP or HTTP(s)) , the archive type (or nil for a plain file) and a specific destination path for the file. Note that compressed files when output will have the siffix removed
;; Usually this will default to  *lispydata-home*.

(defclass remote-file ()
  ((URI                   :accessor get-URI                   :initarg :URI                   :documentation "The full URI of the remote file")
   (name                  :accessor get-name                  :initarg :name                  :documentation "the file name only ")
   (host                  :accessor get-host                  :initarg :host                  :documentation "the hostname of the remote file")
   (archive-type          :accessor get-archive-type          :initarg :archive-type          :documentation "classifies the type of archive if any")
   (archive-filename      :accessor get-archive-filename      :initarg :archive-filename      :documentation "the name of the file we download, prior to any processing")
   
   (archive-member        :accessor get-archive-member        :initarg :archive-member        :documentation "specifies the member file of the archive to extract")
   (file-type             :accessor get-file-type             :initarg :file-type             :documentation "is the file a csv, or other")
   (protocol              :accessor get-protocol              :initarg :protocol              :documentation "HTTP,HTTPS,FTP or File")
   (destination-directory :accessor get-destination-directory :initarg :destination-directory :documentation "The output directory")
   (destination-pathname  :accessor get-destination-pathname  :initarg :destination-pathname  :documentation "the fully qualified name of the final output file")))
;;
;; utilities for class initialisations
;;

(defun %infer-file-type (name-string)
  "return the file type(s) of the file designated by name-string. The order of this is important in the case of things like tar.gz as this drives the decompression process"
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

(defun make-remote-file (file &key (archive-member nil))
  "pull part the file name , create the fully qualified output name and make the directories in the the cache. If it a .tar, .zip or .Z file, ensure that we specify a member to extract.

When archive-member is not-nil, treat that as the name of a file contained in the archive and extract that. When archive-member is nil, we will check the contents of the archive and extract the first member found. "
  (let* ((uri (quri:uri file))
	 (protocol (quri:uri-scheme uri))
	 (name (%uri-file uri))
	 (archive-type (%archive-protocol name))
	 (hostname (quri:uri-host uri))
	 (destination-directory (concatenate 'string
					     *lispydata-home*
					     hostname
					     (%uri-path-only uri)
					     "/"))
	 (destination-pathname (concatenate 'string destination-directory (trim-archive-suffix name archive-type)))
	 (archive-filename (concatenate 'string destination-directory name)))
    ;; (and (member  archive-type '(:tar :tarball :zip))
    ;;    (null archive-member)
    ;;    (error "you must specify a member to extract from the archive"))
    
    (break) (uiop:ensure-all-directories-exist (list destination-pathname))
    
    (make-instance 'remote-file
		   :URI uri
		   :name name
		   :protocol (%protocol protocol)
		   :host hostname
		   :archive-type archive-type
		   :archive-filename archive-filename
		   :archive-member archive-member
		   :file-type (%infer-file-type name)
		   :destination-directory  destination-directory
		   :destination-pathname destination-pathname)))

;;
;; ==================================================
;;
(defgeneric %fetch (file protocol)
  (:documentation "fetch the file from a remote host using the appropriate protocol")
  (:method ((file remote-file)   (protocol (eql :FTP)))
	  (%ftp-retrieve file))
  (:method ((file remote-file) (protocol ( eql :HTTP)))
	  (let ((filename (quri:render-uri (get-uri file))))
	    (trivial-download:download filename (get-archive-filename file))))
  (:method ((file remote-file) (protocol (eql :file)))
    ;; don't copy if the file is already in the cache.
    (unless (probe-file (get-destination-pathname file))
      (uiop:copy-file (get-name file) (get-destination-pathname file)))))



(defun %ftp-retrieve (file &key (port 21) (username "anonymous") (password "lispydata"))
  "This taken from the example cl-ftp client."
  
  (ftp.client:with-ftp-connection (conn :hostname (get-host file) 
			     :port port
			     :username username
			     :password password
			     :passive-ftp-p t)
    (ftp.client:send-cwd-command conn (%uri-path-only (get-uri file)))
    (ftp.client:retrieve-file conn (get-name file)
		   (get-archive-filename file)
		   :type :binary :if-exists :supersede)))


;;
;; =================================
;;
(defun %ensure-archive-member (file)
  "if the slot archive-member is nil, then look inside the archive and take the first member"
  (unless (get-archive-member file)
    (setf (get-archive-member file)
	  (cond
	    ((eql (get-archive-type file) :zip) (first (%list-zip-file-entries file)))
	    ((eql (get-archive-type file) :tar) (first (%list-archive-entries file)))
	    (t (error "%ensure-archive-member invalid archive-type ~a~%" (get-archive-type file))))))
  (get-archive-member file))

(defun %decompress-tar-file (file &optional (tar-only nil))
  "decompress the file and if a member is specified, extract that member. IF the file is just a tar file, don't decompress "
  (unless tar-only
    (%decompress-archive file))
  
  (%extract-archive-member file (%ensure-archive-member file)))



(defgeneric %decompress (file archive-type)
  (:documentation  "For the remote file 'file' apply processing to decompress the archive and where appropriate extract a member from an archive")
  (:method ((file remote-file) (archive-type (eql :ignore)))
    (values))
  (:method ((file remote-file) (archive-type (eql :tar)))
    (%decompress-tar-file file t))
  (:method ((file remote-file) (archive-type (eql :tarball)))
    (%decompress-tar-file file))
  (:method ((file remote-file) (archive-type (eql :zip)))
    (%extract-zipfile-member  file (%ensure-archive-member file)))
  (:method ((file remote-file) (archive-type (eql :bzip)))
    (error "Sorry, bzip not implemented as yet"))
  (:method ((file remote-file) (archive-type (eql :Z )))
    (%decompress-tar-file file)))

(defun %list-zip-file-entries ( file)
  (let (entries)
    (zip:with-zipfile (zip (get-archive-filename file) )
      (zip:do-zipfile-entries (name entry zip)
	(unless (search "__MACOSX" name)
	  (pushnew name entries :test #'string= )) ))
    entries))

(defun %list-archive-entries (file)
  (let (entries)
    (archive:do-archive-entries (entry (get-name  file))
      (pushnew (archive:name entry) entries :test #'string=))))

(defun %extract-zipfile-member (file member)
  (format t "extracting member ~a from zip ~a~%" member (get-name file))
  (zip:with-zipfile (zipfile (get-archive-filename file))
    (alexandria:if-let (zip-entry (zip:get-zipfile-entry member zipfile))
      (with-open-file (outfile (get-destination-pathname file)
			       :direction :output
			       :element-type '(unsigned-byte 8)
			       :if-exists :supersede)
	(zip:zipfile-entry-contents zip-entry outfile))
      (error "could not find member ~a in zip ~a" member (get-name file)))))

(defun  %decompress-and-extract-tarball (file &optional (tar-only nil))
  "Decompress  a tarball (.tar.gz) or a .tar file to a directory . Member files are placed in that directory"
  (with-open-file (tarball-stream (get-archive-filename file)
				  :direction :input
				  :element-type '(unsigned-byte 8))
    ;; the only way to tell archive where to put files is by binding default-pathname-defaults
    (let ((*default-pathname-defaults* (get-destination-directory file) ))
      (archive::extract-files-from-archive
       (archive:open-archive 'archive:tar-archive
			     (if tar-only
				 tarball-stream
				 (chipz:make-decompressing-stream 'chipz:gzip tarball-stream))

			     :direction :input))))
  ;(uiop:delete-file-if-exists (get-name file))
  )


(defparameter *archive-methods-alist* '(( :gzip . chipz:gzip)
					( :zip . chipz:zlib)
					( :bzip . chipz:bzip2)
					( :plain . :ignore))
  "Alist that associates the file type with the decompression methods")



(defun %archive-method-decode (amethod)
  (cdr (assoc amethod *archive-methods-alist*)))

(defun %remove-archive-methods (file-type)
  (remove-if (lambda (method) (%archive-method-decode method)) file-type))

(defun %decompress-archive (file)
  "turn a .gz , bz2 or  .zip file into its uncompressed equivalent"
  (with-open-file  (archive-stream (get-archive-filename file)
				   :direction :input
				   :element-type '(unsigned-byte 8))
    (with-open-file (stream (get-destination-pathname file)
			    :direction :output
			    :element-type '(unsigned-byte 8)
			    :if-exists :supersede)
      (chipz:decompress stream
			(%archive-method-decode (get-archive-type file) )
			archive-stream))))

(defun  %extract-archive-member (file member)
  "Extract a member from a zip or tar file"
  (let ((*default-pathname-defaults* (get-destination-directory file)))
    (format t "extracting member ~a from Archive ~a~%" member (get-name file))
    (archive:with-open-archive (archive (get-archive-filename file))
      (archive:do-archive-entries (entry archive)
	(when (string= (archive:name entry) member)
	  (archive:extract-entry archive entry))))))


;;
;; =====================================================
;; Dataframe stuff


(defparameter *field-type-alist* '((number . dfv::number)
				 (date    . dfv:date)
				 (category . dfv::string)
				 (string . dfv::string))
  "an alist to map df column types to the parsing library")

(defun %process-type (type field)
  "try to guess the type of a field. note the special case of category fields"
  (let ((dfv-type (cdr (assoc type *field-type-alist*))))
    
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
     for spec in (mapcar #'car (remove 'category *field-type-alist* :key #'car))
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
  "this is probably a waste of time"
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
			 :element-type 'keyword))
    (otherwise (make-array 1000
			 :fill-pointer 0
			 :adjustable t
			 :element-type t))))

;;
;; Missing data. This is quite a difficult topic. What we do at the moment is
;;   if the field is blank or it matches *missing-data-regexp* then we will return the symbol :na.
;; This means then basic numeric operations will fail until some recoding work is done.
;; The regexp is exported, so can be rebound as appropriate for different use cases
;; 
(defparameter *missing-data-regexp*  (cl-ppcre:create-scanner  "^\\s*[nN]/?[aA]\\s*$|^\\s*$"))

(defun %missing-data? (field)
  (cl-ppcre:scan *missing-data-regexp* field))

(defun %parse-input-field (type field)
  
  (let ((result  (cond
		   ((%missing-data? field) (list :na t))
		   ((equal type 'category) (list (alexandria:make-keyword field) nil))
		   (t (list (dfv:parse-input  (cdr (assoc type *field-type-alist*)) field) nil )))))
    
    result))

(defun %process-csv-lines (csv-file column-types columns &key (skip nil))
  " read through the lines in the csv-file and process them"
  ;; skip blank lines and the header line.
  (when skip
    (loop while (%blank-csv-line (fare-csv:read-csv-line csv-file))))
  (loop for line = (fare-csv:read-csv-line csv-file)
     until (null line)
     collect (some #'identity
		   (let (parsed-field)
		     (loop 
			for index below (length line) do
			  (setf parsed-field (%parse-input-field (nth index column-types) (nth index line))) 
			  (vector-push-extend (first parsed-field) (nth index columns))
     
			collect (second parsed-field))))))

(Defun %read-csv-guess-spec (file )
  " When its a CSV with column headers use the column headers and then attempt to infer the types of each column."
  (with-open-file ( csv-file (get-destination-pathname file)   :direction :input )
    (let* ((column-names (%collect-column-names-as-keywords csv-file))
	   (column-types (%infer-column-types csv-file))
	   (columns (loop for type in column-types  collect (%make-column-vector type)))
	   (incomplete-cases  (%process-csv-lines csv-file column-types columns )))

     
      (dframe:make-df column-names columns
			 :column-types column-types
			 :incomplete-cases incomplete-cases))))

(defun %read-csv-with-spec (file  spec)
  " "
  (with-open-file ( csv-file (get-destination-pathname file)  :direction :input )
    (let* ((skip (if (member 'skip spec) (setf spec (remove 'skip spec)) nil))
	   (column-names (mapcar #'car spec))
	   (column-types (mapcar #'cdr spec))
	   (columns (loop for type in column-types  collect (%make-column-vector type)))
	   (incomplete-cases (%process-csv-lines csv-file column-types columns :skip skip)))
      
      (dframe:make-df column-names columns
		      :column-types column-types
		      :incomplete-cases incomplete-cases))))
;;
;; a CSV spec is  a plist of the form '( (:skip | :noheading)  (colname1 . :type) (Colname2 . :type) ....)
;; where skip means skip existing column headings
;;       noheading means don't look for column headings
;;       type is one of
;;       number  - put into a type t vector, this may be coerced into double-floats
;;       date    - a fixnum
;;;      category - symbol
;;;      string  - string.
;;; if you don;t use a spec for the csv it does its best to guess types. 
;; Note: If your file does not have column names, you must use a spec
;; Note: we can't guess categorical variables very well. So we don't try . If a column is categorical you have to use a spec to denote it

(defun %csv-to-df (file &optional (spec nil))
  
  (if spec
      (%read-csv-with-spec file spec)
      (%read-csv-guess-spec file)))
;;
;; a flat file spec is a superset of a CSV spec. We can't infer field
;; positions in a flat file so it must be supplied. for is similar to CSV
;; a list of the form  '(fieldname type length     ) where type is the same as a CSV spec
;;

(defun %split-line-at-position (line positions widths)
  (loop for index below (length positions) 
      for pos = (pop positions) 
       for width = (pop widths)
       collect (subseq line pos (+ pos width)  ))
  )

(defun %process-flat-lines (flat-file column-types column-widths columns)
  " read through the lines in the file and process them"
 
  (let ((column-start (let ((start 0))
			(mapcar (lambda ( next)
				  (prog1 start
				    (incf start next))) column-widths))))
    
    (loop for line = (read-line flat-file nil nil)
       until (null line)
       collect (some #'identity
		     (let ((parsed-field)
			   (line (%split-line-at-position line column-start column-widths)))
		       (loop 
			  for index below (length line) do
			    (setf parsed-field (%parse-input-field (nth index column-types) (nth index line))) 
			    (vector-push-extend (first parsed-field) (nth index columns))
			    
			  collect (second parsed-field)))))))



(defun %flat-file-to-df (file  spec)
  " "
  (with-open-file ( flat-file (get-destination-pathname file)  :direction :input )
    (let* ((skip (if (member 'skip spec) (setf spec (remove 'skip spec)) nil))
	   (column-names (mapcar #'car spec))
	   (column-types (mapcar #'cadr spec))
	   (column-widths (mapcar #'caddr spec ))
	   
	   (columns (loop for type in column-types  collect (%make-column-vector type)))
	   (incomplete-cases (%process-flat-lines flat-file column-types column-widths columns )))
      
      (dframe:make-df column-names columns
		      :column-types column-types
		      :incomplete-cases incomplete-cases))))

(defparameter *lisp-to-df-types* '(
				   (BIT . NUMBER)
				   (SIMPLE-ARRAY . STRING)
				   (INTEGER . NUMBER)
				   (FIXNUM . NUMBER)
				   (FLOAT . NUMBER)
				   (DOUBLE-FLOAT .  NUMBER)))
(defun %ensure-list (list)
  (if (consp list)
      list
      (list list)))

(defun %lisp-to-df-type (key obj)
  (let ((the-type (first (%ensure-list (type-of (cdr (assoc key obj)))))))

        (cdr (assoc the-type *lisp-to-df-types*))))

(defun %infer-json-spec (json-obj)
  "this is a slack attempt at determining column names and types from an input JSOn file. This assumes that the input file has a very simple, flat structure with no nested objects. It also assumes that all json objects are the same of course. categorical variables will need to be recoded."
  (let* ((json-names  (mapcar #'car json-obj))
	 (json-types  (loop for n in json-names collect (%lisp-to-df-type n json-obj))))
    (mapcar #'list json-names json-types)))

(defun %process-json (json columns)
  " "
  
  (loop for obj in json 
    
     collect (some #'identity
		   (loop 
		      for field in  obj 
		      for index from 0 do
			 
			(vector-push-extend (cdr field ) (nth index columns))
			
		      collect t)))) ; a hack 

(defun %json-to-df (remote-file)
  "this is just a proof of concept and will barf pretty quickly on large inputs"
  (let* ((json (with-open-file (json-file  (get-destination-pathname remote-file)  :direction :input)
		 (cl-json:decode-json  json-file)))
	 (json-spec (%infer-json-spec (first json)))
	 (column-names (mapcar #'first json-spec))
	 (column-types (mapcar #'second json-spec))
	 (columns (loop for type in column-types  collect (%make-column-vector type)))
	   (incomplete-cases (%process-json json    columns )))
	 
    
    (dframe:make-df column-names columns
		    :column-types column-types
		    :incomplete-cases incomplete-cases)))

(defun %file-to-df (remote-file &optional (spec nil ))
  "convert a file into a dataframe. "
  (ecase (first (%remove-archive-methods (%infer-file-type (get-name remote-file))))
    (:csv (%csv-to-df remote-file spec ))
    (:flat (%flat-file-to-df remote-file spec) )
    (:json (%json-to-df remote-file))))

(defun fetch (file &key (spec nil) (cache nil) (archive-member nil))
  "fetch a file, unpack it if needed and then if its a csv turn it into a dataframe. If cache non nil, don't do the download"
  (unless (stringp file)
    (error "Fetch: Parameter 'file' must be a string"))
  (let ((file-to-fetch (make-remote-file file :archive-member archive-member)))
       (unless cache
	 (%fetch file-to-fetch (get-protocol file-to-fetch))
	 (%decompress file-to-fetch (get-archive-type file-to-fetch)))
       (%file-to-df file-to-fetch spec)))


(defun generate-csv-spec (file )
  "Conveninece function to generate a valid spec to read a CSV, makes it easier to create a spec when we can't guess the contents"
  (with-open-file ( csv-file  file   :direction :input )
    (let* ((column-names (%collect-column-names-as-keywords csv-file))
	   (column-types (%infer-column-types csv-file)))
      
      (mapcar #'cons column-names column-types))))
