Assignment 6
====================

### Remark ###
# The output files of Q1 is commited (output_1).
First, run the following command

spark-submit --class ca.uwaterloo.cs451.a7.EventCount  target/assignments-1.0.jar --input /u3/cs451/public_html/taxi-data --checkpoint cs451-lintool-checkpoint-q1 --output output_1/cs451-1-output-q1

Then output to console

find output_1/cs451-1-output-q1-* -name "part*" | xargs grep 'all' | sed -E 's/^output-([0-9]+)\/part-[0-9]+/\1/' | sort -n

output:


utput_1/cs451-1-output-q1-10800000/part-00033:(all,3605)
output_1/cs451-1-output-q1-14400000/part-00033:(all,2426)
output_1/cs451-1-output-q1-18000000/part-00033:(all,2505)
output_1/cs451-1-output-q1-21600000/part-00033:(all,3858)
output_1/cs451-1-output-q1-25200000/part-00033:(all,10258)
output_1/cs451-1-output-q1-28800000/part-00033:(all,19007)
output_1/cs451-1-output-q1-32400000/part-00033:(all,23799)
output_1/cs451-1-output-q1-3600000/part-00033:(all,7396)
output_1/cs451-1-output-q1-36000000/part-00033:(all,24003)
output_1/cs451-1-output-q1-39600000/part-00033:(all,21179)
output_1/cs451-1-output-q1-43200000/part-00033:(all,20219)
output_1/cs451-1-output-q1-46800000/part-00033:(all,20522)
output_1/cs451-1-output-q1-50400000/part-00033:(all,20556)
output_1/cs451-1-output-q1-54000000/part-00033:(all,21712)
output_1/cs451-1-output-q1-57600000/part-00033:(all,22016)
output_1/cs451-1-output-q1-61200000/part-00033:(all,18034)
output_1/cs451-1-output-q1-64800000/part-00033:(all,19719)
output_1/cs451-1-output-q1-68400000/part-00033:(all,25563)
output_1/cs451-1-output-q1-7200000/part-00033:(all,5780)
output_1/cs451-1-output-q1-72000000/part-00033:(all,28178)
output_1/cs451-1-output-q1-75600000/part-00033:(all,27449)
output_1/cs451-1-output-q1-79200000/part-00033:(all,27072)
output_1/cs451-1-output-q1-82800000/part-00033:(all,24078)
output_1/cs451-1-output-q1-86400000/part-00033:(all,18806)
 
