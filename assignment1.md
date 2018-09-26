Assignment 0
====================

### Q1

Pairs: 2 MP jobs. Job1: input the origin file. It has two intermediate key pairs, one is (x, y) for calculating co-occurence and another is (x, *) for calculating the number of lines that including x. The intermediate value pair is (Float, int). The output record is ((x,y) (float, int)) which is also input for Job2. The output record of Job2 is ((x,y) (PMI, co-occurence))
Stripes: 2 MP jobs. Job1 and intermediate key-value pairs: same as Pairs. The final output record for Job2 is a map from Text to HashMapWritable(<Text, PairOfFloatInt>).


### Q2
Pairs:26.968+2.553
Stripes: 27.076+2.499

linux.student.cs.uwaterloo.ca

### Q3
Pairs:29.133+2.610
Stripes: 30.338+2.502

linux.student.cs.uwaterloo.ca

### Q4
77198 in total (counted both (x,y) and (y,x))


### Q5
(maine, anjou)    (3.6331422)
(you, thy)    (-1.5303968)

By the definition of PMI, we know that "maine" and "anjou" are the most likely appear in the same line and "you" and "thy" are least likely to appear in the same line.



### Q6
(tears, salt)	(2.0528123, 11)
(tears, shed)	(2.1117902, 15)
(tears, eyes)	(1.165167, 23)

(die, death)	(0.7541594, 18)
(life, death)	(0.73813456, 31)
(father's, death)	(1.120252, 21)



### Q7
(hockey, defenceman)	(2.4180872, 153)
(hockey, ice)	(2.2093477, 2160)
(hockey, sledge)	(2.352185, 93)
(hockey, winger)	(2.3700917, 188)
(hockey, goaltender)	(2.2537384, 199)


### Q8
(data, encryption)	(2.0443723, 53)
(data, cooling)	(2.0979042, 74)
(data, array)	(1.9926307, 50)
(data, storage)	(1.9878386, 110)
(data, database)	(1.8893089, 99)

