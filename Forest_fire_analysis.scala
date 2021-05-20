val forestFireRdd = sc.textFile("C:\\Users\\Anuvarshini\\Desktop\\sem 4\\EVS\\amazon.csv")
val header = forestFireRdd.first()
val forestFire = forestFireRdd.filter(l => l != header)
val forestFireSchemeRdd = forestFire.map{l =>
val str = l.split(',')
val (year,state,month,num,date) = (str(0),str(1),str(2),str(3).toFloat,str(4))
(year,state,month,num,date)}.filter(l=> l._4 != 0)

val fireCountPerYear = forestFireSchemeRdd.map{case(year,state,month,num,date) =>
(year,num.round)}.reduceByKey(_+_).sortBy(-_._2)

fireCountPerYear.toDF("Year","Forest Fires").show()

val fireCountPerState = forestFireSchemeRdd.map{case(year,state,month,num,date) =>
(state,num.round)}.reduceByKey(_+_).sortBy(-_._2)

fireCountPerState.toDF("State","Forest Fires").show()

val fireMaxStateYear = forestFireSchemeRdd.map{case(year,state,month,num,date) =>
((year,state),num)}.reduceByKey(math.max(_, _)).sortBy(-_._2).map{l => (l._1._1,l._1._2,l._2)}

fireMaxStateYear.toDF("Year","State","Fires").show()


/*val file = new java.io.PrintStream("C:\\Users\\Anuvarshini\\Desktop\\sem 4\\EVS\\firePerYear.csv")
fireCountPerYear.collect.foreach{file.println(_)}
file.close

val file1 = new java.io.PrintStream("C:\\Users\\Anuvarshini\\Desktop\\sem 4\\EVS\\firePerState.csv")
fireCountPerState.collect.foreach{file1.println(_)}
file1.close

val file2 = new java.io.PrintStream("C:\\Users\\Anuvarshini\\Desktop\\sem 4\\EVS\\firePerYearState.csv")
fireMaxStateYear.collect.foreach{file2.println(_)}
file2.close*/
