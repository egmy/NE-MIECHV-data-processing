* Encoding: UTF-8.

* SPSS Restructuring code:
SORT CASES BY ProjectID .
CASESTOVARS
  /ID=ProjectID
  /GROUPBY=INDEX.
