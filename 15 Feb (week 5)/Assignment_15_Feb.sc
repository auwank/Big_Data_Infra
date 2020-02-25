import scala.collection.mutable.ListBuffer

object Assignmnt_15_Feb {
  
   // 1- Write some code that prints out the first 10 values of the Fibonacci sequence.
	 // This is the sequence where every number is the sum of the two numbers before it.
	 // So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
	
	 var a = 0                                //> a  : Int = 0
	 var b = 1                                //> b  : Int = 1
	 var fib = 1                              //> fib  : Int = 1
	for (x <- 1 to 10) {
		if (x==1) {println(a)}
		else {
		println(fib)
			fib = a+b
		  a=b
		  b=fib
		}                                 //> 0
                                                  //| 1
                                                  //| 1
                                                  //| 2
                                                  //| 3
                                                  //| 5
                                                  //| 8
                                                  //| 13
                                                  //| 21
                                                  //| 34
 		}
 		
 		// 3- Write a max function that picks the max of two numbers and
  // another callmax function to call the first one with inputs
  
  def max(x:Int, y:Int) : Int = {
  if (x>y){x}
  else {y}
  }                                               //> max: (x: Int, y: Int)Int
  
   def callmax(x: Int,y: Int, f: (Int,Int) => Int) : Int = {
  	max(x,y)
  }                                               //> callmax: (x: Int, y: Int, f: (Int, Int) => Int)Int
  println(callmax(99,100,max))                    //> 100
  
  
  //Write a function to compute factorial (5! = 5*4*3*2*1)
  //Then write another function to call fact function and println few examples (i.e, 6,8,4.52)
  
  def fact(x:Int) : Int = {
  	var p = 1
   	for (a <- 1 to x) {
 		p = p*a
 		}
 		p
  }                                               //> fact: (x: Int)Int
   
   def call_fact(x: Int, f: Int => Int) : Int = {
  	fact(x)
  }                                               //> call_fact: (x: Int, f: Int => Int)Int
  println (call_fact(4, fact))                    //> 24
  println (call_fact(5, fact))                    //> 120
  println (call_fact(6, fact))                    //> 720
  
  
 // 2-a) Then write another function to compute the factorial via reading from list. For
//instance, you will get list as (1,2,3,4,5) then multiply them together and compute the
//factorial.
  
  val lst2 = ListBuffer(1,2,3)                    //> lst2  : scala.collection.mutable.ListBuffer[Int] = ListBuffer(1, 2, 3)

	  def fact_lst(x:ListBuffer[Int]) : Int = {
	   var p = 1
	   for (i <- 1 to x.length){
	   p=p*x(i-1)
	   }
  	fact(p)
  }                                               //> fact_lst: (x: scala.collection.mutable.ListBuffer[Int])Int
  
  fact_lst(lst2)                                  //> res0: Int = 720
  
  
  //Use the reduce method and re-compute the factorial number.

fact(lst2.reduce((x, y) => x * y))                //> res1: Int = 720


// 2-c) Extend the previous code to generate a list from a number (6 turns into
// list(1,2,3,4,5,6)) then compute the factorial.

  def cr_lst(x:Int) : Int = {
	   var lst_1 = ListBuffer[Int]()
	   for (i <- 1 to x){
	   lst_1 += i
	   }
  	fact_lst(lst_1)
  	}                                         //> cr_lst: (x: Int)Int
cr_lst(3)                                         //> res2: Int = 720

 // Exercise 3
//Generate a list from 1 to 45 then apply .filter to compute the following results:
	
 		var lst_1 = ListBuffer[Int]()     //> lst_1  : scala.collection.mutable.ListBuffer[Int] = ListBuffer()
 		
 	for (i <- 1 to 45){
	   lst_1 += i
	   }
  println(lst_1)                                  //> ListBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 1
                                                  //| 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 
                                                  //| 38, 39, 40, 41, 42, 43, 44, 45)
   
 //● Sum of the numbers divisible by 4;
  val evens = lst_1.filter(_ % 4 == 0).reduce((x, y) => x + y)
                                                  //> evens  : Int = 264
  //● Sum of the squares of the numbers divisible by 3 and less than 20;
  val even_2 = lst_1.filter(_ <20).filter(_ % 3 == 0).reduce((x, y) => (x*x) + (y*y))
                                                  //> even_2  : Int = 1225318693


}