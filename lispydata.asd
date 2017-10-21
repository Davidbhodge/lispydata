;;;; lispydata.asd

(asdf:defsystem #:lispydata
  :description "Data Manipulation in lisp "
  :author "David Hodge"
  :license "WTFNMF"
  :serial t
  :depends-on ( #:ftp
		#:alexandria
		#:trivial-download
		#:quri
		#:zip
		#:chipz
		#:archive
		#:fare-csv
		#:data-format-validation
		#:cl-data-frame
		#:cl-json
		)
  :components ((:file "package")
               (:file "lispydata")
	       (:file "fetch")))

