
Setup :

I only looked at the directory edrm-enron-v2

I unzipped all the xml related zip files where the xml file had
the record of all emails and addressees and contained folders named text_000
etc containg the email texts.  All the xml files and the text emails were pushed
to separate s3 buckets.

After several  experiments and blind alleys I decided on  parsing the TagValues and
extracting the attribute values  of the To and CC attributes
and using a regex to get a list of valid email addresses of the
type name@company.com  these were then weighted and aggregated
to produce a list of most emailed persons  for
the sample set I worked on



(harry.arora@enron.com,351.5)
(james.d.steffes@enron.com,390.5)
(sally.beck@enron.com,613.5)
(jdasovic@enron.com,772.0)
(michelle.cash@enron.com,896.0)
(jeff.dasovich@enron.com,1734.0)

was a typical result

I had issues with my Amazon VPC and was not able to launch a cluster
to do full scale testing.


SBT is the build tool
sbt clean compile pacakge will build a deployable jar
which can be run on a cluster with emailxmls , emailtexts directories replaced with S3 Buckets
of the specified file sets

Due to the issues I had with getting the extraction of email attributes
I did not have time to do proper and sufficient testing .


vr
Hugh McBride


